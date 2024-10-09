import time
import socket
from sys import platform
import zlib
import struct

import raft_pysync.pickle as pickle

from .poller import POLL_EVENT_TYPE
from time import monotonic as monotonicTime


class ConnectionState:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2


def _get_addr_type(addr):
    try:
        socket.inet_aton(addr)
        return socket.AF_INET
    except socket.error:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, addr)
        return socket.AF_INET6
    except socket.error:
        pass
    raise Exception("unknown address type")


import socket


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def set_keepalive_osx(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    tcp_keepalive = 0x10
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, tcp_keepalive, interval_sec)


def set_keepalive_windows(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    sock.ioctl(
        socket.SIO_KEEPALIVE_VALS, (1, after_idle_sec * 1000, interval_sec * 1000)
    )


def set_keepalive(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    if platform == "linux" or platform == "linux2":
        set_keepalive_linux(sock, after_idle_sec, interval_sec, max_fails)
    elif platform == "darwin":
        set_keepalive_osx(sock, after_idle_sec, interval_sec, max_fails)
    elif platform == "win32":
        set_keepalive_windows(sock, after_idle_sec, interval_sec, max_fails)


class TcpConnection(object):

    def __init__(
        self,
        poller,
        on_message_received=None,
        on_connected=None,
        on_disconnected=None,
        socket=None,
        timeout=10.0,
        send_buffer_size=2 ** 13,
        recv_buffer_size=2 ** 13,
        keepalive=None,
    ):
        self.sendRandKey = None
        self.recvRandKey = None
        self.recvLastTimestamp = 0
        self.encryptor = None

        self.__socket = socket
        self.__readBuffer = bytes()
        self.__writeBuffer = bytes()
        self.__lastReadTime = monotonicTime()
        self.__timeout = timeout
        self.__poller = poller
        self.__keepalive = keepalive
        if socket is not None:
            self.__socket = socket
            self.__fileno = socket.fileno()
            self.__state = ConnectionState.CONNECTED
            self.set_socket_keepalive()
            self.__poller.subscribe(
                self.__fileno,
                self.__process_connection,
                POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.WRITE | POLL_EVENT_TYPE.ERROR,
            )
        else:
            self.__state = ConnectionState.DISCONNECTED
            self.__fileno = None
            self.__socket = None

        self.__onMessageReceived = on_message_received
        self.__onConnected = on_connected
        self.__onDisconnected = on_disconnected
        self.__sendBufferSize = send_buffer_size
        self.__recvBufferSize = recv_buffer_size

    def set_socket_keepalive(self):
        if self.__socket is None:
            return
        if self.__keepalive is None:
            return
        set_keepalive(
            self.__socket,
            self.__keepalive[0],
            self.__keepalive[1],
            self.__keepalive[2],
        )

    def set_on_connected_callback(self, on_connected):
        self.__onConnected = on_connected

    def set_on_message_received_callback(self, on_message_received):
        self.__onMessageReceived = on_message_received

    def set_on_disconnected_callback(self, on_disconnected):
        self.__onDisconnected = on_disconnected

    def connect(self, host, port):
        if host is None:
            return False
        self.__state = ConnectionState.DISCONNECTED
        self.__fileno = None
        self.__socket = socket.socket(_get_addr_type(host), socket.SOCK_STREAM)
        self.__socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize
        )
        self.__socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize
        )
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.set_socket_keepalive()
        self.__socket.setblocking(0)
        self.__readBuffer = bytes()
        self.__writeBuffer = bytes()
        self.__lastReadTime = monotonicTime()

        try:
            self.__socket.connect((host, port))
        except socket.error as e:
            if e.errno not in (socket.errno.EINPROGRESS, socket.errno.EWOULDBLOCK):
                return False
        self.__fileno = self.__socket.fileno()
        self.__state = ConnectionState.CONNECTING
        self.__poller.subscribe(
            self.__fileno,
            self.__process_connection,
            POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.WRITE | POLL_EVENT_TYPE.ERROR,
        )
        return True

    def send(self, message):
        if self.sendRandKey:
            message = (self.sendRandKey, message)
        data = zlib.compress(pickle.dumps(message), 3)
        if self.encryptor:
            data = self.encryptor.encrypt_at_time(data, int(monotonicTime()))
        data = struct.pack("i", len(data)) + data
        self.__writeBuffer += data
        self.__try_send_buffer()

    def fileno(self):
        return self.__fileno

    def disconnect(self):
        need_call_disconnect = False
        if (
            self.__onDisconnected is not None
            and self.__state != ConnectionState.DISCONNECTED
        ):
            need_call_disconnect = True
        self.sendRandKey = None
        self.recvRandKey = None
        self.recvLastTimestamp = 0
        if self.__socket is not None:
            self.__socket.close()
            self.__socket = None
        if self.__fileno is not None:
            self.__poller.unsubscribe(self.__fileno)
            self.__fileno = None
        self.__writeBuffer = bytes()
        self.__readBuffer = bytes()
        self.__state = ConnectionState.DISCONNECTED
        if need_call_disconnect:
            self.__onDisconnected()

    def get_send_buffer_size(self):
        return len(self.__writeBuffer)

    def __process_connection(self, descr, event_type):
        poller = self.__poller
        if descr != self.__fileno:
            poller.unsubscribe(descr)
            return

        if event_type & POLL_EVENT_TYPE.ERROR:
            self.disconnect()
            return

        self.__process_connection_timeout()
        if self.state == ConnectionState.DISCONNECTED:
            return

        if event_type & POLL_EVENT_TYPE.READ or event_type & POLL_EVENT_TYPE.WRITE:
            if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                self.disconnect()
                return

            if self.__state == ConnectionState.CONNECTING:
                if self.__onConnected is not None:
                    self.__onConnected()
                if self.__state == ConnectionState.DISCONNECTED:
                    return
                self.__state = ConnectionState.CONNECTED
                self.__lastReadTime = monotonicTime()
                return

        if event_type & POLL_EVENT_TYPE.WRITE:
            self.__try_send_buffer()
            if self.__state == ConnectionState.DISCONNECTED:
                return
            event = POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.ERROR
            if len(self.__writeBuffer) > 0:
                event |= POLL_EVENT_TYPE.WRITE
            poller.subscribe(descr, self.__process_connection, event)

        if event_type & POLL_EVENT_TYPE.READ:
            self.__try_read_buffer()
            if self.__state == ConnectionState.DISCONNECTED:
                return

            while True:
                message = self.__process_parse_message()
                if message is None:
                    break
                if self.__onMessageReceived is not None:
                    self.__onMessageReceived(message)
                if self.__state == ConnectionState.DISCONNECTED:
                    return

    def __process_connection_timeout(self):
        if monotonicTime() - self.__lastReadTime > self.__timeout:
            self.disconnect()
            return

    def __try_send_buffer(self):
        self.__process_connection_timeout()
        if self.state == ConnectionState.DISCONNECTED:
            return
        while self.__process_send():
            pass

    def __process_send(self):
        if not self.__writeBuffer:
            return False
        try:
            res = self.__socket.send(self.__writeBuffer)
            if res < 0:
                self.disconnect()
                return False
            if res == 0:
                return False
            self.__writeBuffer = self.__writeBuffer[res:]
            return True
        except socket.error as e:
            if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                self.disconnect()
            return False

    def __try_read_buffer(self):
        while self.__process_read():
            pass
        self.__lastReadTime = monotonicTime()

    def __process_read(self):
        try:
            incoming = self.__socket.recv(self.__recvBufferSize)
        except socket.error as e:
            if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                self.disconnect()
            return False
        if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
            self.disconnect()
            return False
        if not incoming:
            self.disconnect()
            return False
        self.__readBuffer += incoming
        return True

    def __process_parse_message(self):
        if len(self.__readBuffer) < 4:
            return None
        l = struct.unpack("i", self.__readBuffer[:4])[0]
        if len(self.__readBuffer) - 4 < l:
            return None
        data = self.__readBuffer[4 : 4 + l]
        try:
            if self.encryptor:
                data_timestamp = self.encryptor.extract_timestamp(data)
                assert data_timestamp >= self.recvLastTimestamp
                self.recvLastTimestamp = data_timestamp
                # Unfortunately we can't get a timestamp and data in one go
                data = self.encryptor.decrypt(data)
            message = pickle.loads(zlib.decompress(data))
            if self.recvRandKey:
                rand_key, message = message
                assert rand_key == self.recvRandKey
        except:
            # Why no logging of security errors?
            self.disconnect()
            return None
        self.__readBuffer = self.__readBuffer[4 + l :]
        return message

    @property
    def state(self):
        return self.__state
