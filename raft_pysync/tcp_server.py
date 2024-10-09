import socket

from .poller import POLL_EVENT_TYPE
from .tcp_connection import TcpConnection, _get_addr_type


class ServerState:
    UNBOUNDED = (0,)
    BONDED = 1


class TcpServer(object):

    def __init__(
        self,
        poller,
        host,
        port,
        on_new_connection,
        send_buffer_size=2 ** 13,
        recv_buffer_size=2 ** 13,
        connection_timeout=3.5,
        keepalive=None,
    ):
        self.__poller = poller
        self.__host = host
        self.__port = int(port)
        self.__hostAddrType = _get_addr_type(host)
        self.__sendBufferSize = send_buffer_size
        self.__recvBufferSize = recv_buffer_size
        self.__socket = None
        self.__fileno = None
        self.__keepalive = keepalive
        self.__state = ServerState.UNBOUNDED
        self.__onNewConnectionCallback = on_new_connection
        self.__connectionTimeout = connection_timeout

    def bind(self):
        self.__socket = socket.socket(self.__hostAddrType, socket.SOCK_STREAM)
        self.__socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize
        )
        self.__socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize
        )
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__socket.setblocking(False)
        self.__socket.bind((self.__host, self.__port))
        self.__socket.listen(5)
        self.__fileno = self.__socket.fileno()
        self.__poller.subscribe(
            self.__fileno,
            self.__on_new_connection,
            POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.ERROR,
        )
        self.__state = ServerState.BONDED

    def unbind(self):
        self.__state = ServerState.UNBOUNDED
        if self.__fileno is not None:
            self.__poller.unsubscribe(self.__fileno)
            self.__fileno = None
        if self.__socket is not None:
            self.__socket.close()

    def __on_new_connection(self, descr, event):
        if event & POLL_EVENT_TYPE.READ:
            try:
                sock, addr = self.__socket.accept()
                sock.setsockopt(
                    socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize
                )
                sock.setsockopt(
                    socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize
                )
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setblocking(0)
                conn = TcpConnection(
                    poller=self.__poller,
                    socket=sock,
                    timeout=self.__connectionTimeout,
                    send_buffer_size=self.__sendBufferSize,
                    recv_buffer_size=self.__recvBufferSize,
                    keepalive=self.__keepalive,
                )
                self.__onNewConnectionCallback(conn)
            except socket.error as e:
                if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                    self.unbind()
                    return

        if event & POLL_EVENT_TYPE.ERROR:
            self.unbind()
            return
