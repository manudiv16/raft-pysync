from .config import FailReason
from .dns_resolver import global_dns_resolver
from time import monotonic as monotonic_time
from .node import Node, TCPNode
from .tcp_connection import TcpConnection, ConnectionState
from .tcp_server import TcpServer
import functools
import os
import threading
import time
import random


class TransportNotReadyError(Exception):
    """Transport failed to get ready for operation."""


class Transport(object):
    """Base class for implementing transport between RaftPySync nodes"""

    def __init__(self, raft_pysync_object, self_node, other_nodes):
        """
        Initialise the transport

        :param raft_pysync_object: RaftPysyncObject
        :type raft_pysync_object: RaftPysyncObject
        :param self_node: current server node, or None if this is a read-only node
        :type self_node: Node or None
        :param other_nodes: partner nodes
        :type other_nodes: list of Node
        """

        self._onMessageReceivedCallback = None
        self._onNodeConnectedCallback = None
        self._onNodeDisconnectedCallback = None
        self._onReadonlyNodeConnectedCallback = None
        self._onReadonlyNodeDisconnectedCallback = None
        self._onUtilityMessageCallbacks = {}

    def set_on_message_received_callback(self, callback):
        """
        Set the callback for when a message is received, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node, message: any) or None
        """

        self._onMessageReceivedCallback = callback

    def set_on_node_connected_callback(self, callback):
        """
        Set the callback for when the connection to a (non-read-only) node is established, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onNodeConnectedCallback = callback

    def set_on_node_disconnected_callback(self, callback):
        """
        Set the callback for when the connection to a (non-read-only) node is terminated or is considered dead, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onNodeDisconnectedCallback = callback

    def set_on_readonly_node_connected_callback(self, callback):
        """
        Set the callback for when a read-only node connects, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onReadonlyNodeConnectedCallback = callback

    def set_on_readonly_node_disconnected_callback(self, callback):
        """
        Set the callback for when a read-only node disconnects (or the connection is lost), or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onReadonlyNodeDisconnectedCallback = callback

    def set_on_utility_message_callback(self, message, callback):
        """
        Set the callback for when an utility message is received, or disable callback by passing None

        :param message: the utility message string (add, remove, set_version, and so on)
        :type message: str
        :param callback: callback
        :type callback: function(message: list, callback: function) or None
        """

        if callback:
            self._onUtilityMessageCallbacks[message] = callback
        elif message in self._onUtilityMessageCallbacks:
            del self._onUtilityMessageCallbacks[message]

    # Helper functions so you don't need to check for the callbacks manually in subclasses
    def _on_message_received(self, node, message):
        if self._onMessageReceivedCallback is not None:
            self._onMessageReceivedCallback(node, message)

    def _on_node_connected(self, node):
        if self._onNodeConnectedCallback is not None:
            self._onNodeConnectedCallback(node)

    def _on_node_disconnected(self, node):
        if self._onNodeDisconnectedCallback is not None:
            self._onNodeDisconnectedCallback(node)

    def _on_readonly_node_connected(self, node):
        if self._onReadonlyNodeConnectedCallback is not None:
            self._onReadonlyNodeConnectedCallback(node)

    def _on_readonly_node_disconnected(self, node):
        if self._onReadonlyNodeDisconnectedCallback is not None:
            self._onReadonlyNodeDisconnectedCallback(node)

    def try_get_ready(self):
        """
        Try to get the transport ready for operation. This may for example mean binding a server to a port.

        :raises TransportNotReadyError: if the transport fails to get ready for operation
        """

    @property
    def ready(self):
        """
        Whether the transport is ready for operation.

        :rtype bool
        """

        return True

    def wait_ready(self):
        """
        Wait for the transport to be ready.

        :raises TransportNotReadyError: if the transport fails to get ready for operation
        """

    def add_node(self, node):
        """
        Add a node to the network

        :param node node to add
        :type node Node
        """

    def drop_node(self, node):
        """
        Remove a node from the network (meaning connections, buffers, etc. related to this node can be dropped)

        :param node node to drop
        :type node Node
        """

    def send(self, node, message):
        """
        Send a message to a node.
        The message should be picklable.
        The return value signifies whether the message is thought to have been sent successfully. It does not necessarily mean that the message actually arrived at the node.

        param node target node
        :type node
        :param message
        :type message any
        :returns success
        :rtype bool
        """

        raise NotImplementedError

    def destroy(self):
        """
        Destroy the transport
        """


def _utility_callback(res, err, conn, args):
    """
    Callback for the utility messages

    :param res: result of the command
    :param err: error code (one of raft_pysync.config.FAIL_REASON)
    :param conn: utility connection
    :param args: command with arguments
    """

    if not (err is None and res):
        cmdResult = "SUCCESS" if err == FailReason.SUCCESS else "FAIL"
        res = " ".join(map(str, [cmdResult] + args))
    conn.send(res)


class TCPTransport(Transport):
    def __init__(self, raft_pysync_object, self_node, other_nodes):
        """
        Initialise the TCP transport. On normal (non-read-only) nodes, this will start a TCP server. On all nodes, it will initiate relevant connections to other nodes.

        :param raft_pysync_object: RaftPysyncObject        :type raft_pysync_object:
        :param self_node: current node (None if this is a read-only node)
        :type self_node: TCPNode or None
        :param other_nodes: partner nodes
        :type other_nodes: iterable of TCPNode
        """

        super(TCPTransport, self).__init__(raft_pysync_object, self_node, other_nodes)
        self._RaftPysyncObject = raft_pysync_object
        self._server = None
        self._connections = {}  # Node object -> TcpConnection object
        self._unknown_connections = set()  # set of TcpConnection objects
        self._self_node = self_node
        self._self_is_readonly_node = self_node is None
        self._nodes = set()  # set of TCPNode
        self._readonly_nodes = set()  # set of Node
        self._node_addr_to_node = (
            {}
        )  # node ID/address -> TCPNode (does not include read-only nodes)
        self._last_connect_attempt = {}  # TPCNode -> float (seconds since epoch)
        self._prevent_connect_nodes = (
            set()
        )  # set of TCPNode to which no (re)connection should be triggered on _connect_if_necessary; used via drop_node and destroy to cleanly remove a node
        self._readonly_nodes_counter = 0
        self._last_bind_attempt_time = 0
        self._bind_attempts = 0
        self._bind_over_event = (
            threading.Event()
        )  # gets triggered either when the server has either been bound correctly or when the number of bind attempts exceeds the config value maxBindRetries
        self._ready = False
        self._send_random_sleep_duration = 0

        self._RaftPysyncObject.add_on_tick_callback(self._on_tick)

        for node in other_nodes:
            self.add_node(node)

        if not self._self_is_readonly_node:
            self._create_server()
        else:
            self._ready = True

    def _conn_to_node(self, conn):
        """
        Find the node to which a connection belongs.

        :param conn: connection object
        :type conn: TcpConnection
        :returns corresponding node or None if the node cannot be found
        :rtype Node or None
        """

        for node in self._connections:
            if self._connections[node] is conn:
                return node
        return None

    def try_get_ready(self):
        """
        Try to bind the server if necessary.

        raises TransportNotReadyError if the server could not be bound
        """

        self._maybe_bind()

    @property
    def ready(self) -> bool:
        return self._ready

    def _create_server(self):
        """
        Create the TCP server (but don't bind yet)
        """

        conf = self._RaftPysyncObject.conf
        bind_addr = conf.bind_address
        self_addr = getattr(self._self_node, "address")
        if bind_addr is not None:
            host, port = bind_addr.rsplit(":", 1)
        elif self_addr is not None:
            host, port = self_addr.rsplit(":", 1)
            if ":" in host:
                host = "::"
            else:
                host = "0.0.0.0"
        else:
            raise RuntimeError("Unable to determine bind address")

        if host != "0.0.0.0":
            host = global_dns_resolver().resolve(host)
        self._server = TcpServer(
            self._RaftPysyncObject._poller,
            host,
            port,
            on_new_connection=self._on_new_incoming_connection,
            send_buffer_size=conf.send_buffer_size,
            recv_buffer_size=conf.recv_buffer_size,
            connection_timeout=conf.connection_timeout,
        )

    def _maybe_bind(self):
        """
        Bind the server unless it is already bound, this is a read-only node, or the last attempt was too recently.

        raises TransportNotReadyError if the bind attempt fails
        """

        if (
            self._ready
            or self._self_is_readonly_node
            or monotonic_time()
            < self._last_bind_attempt_time + self._RaftPysyncObject.conf.bind_retry_time
        ):
            return
        self._last_bind_attempt_time = monotonic_time()
        try:
            self._server.bind()
        except Exception as e:
            self._bind_attempts += 1
            if (
                self._RaftPysyncObject.conf.max_bind_retries
                and self._bind_attempts >= self._RaftPysyncObject.conf.max_bind_retries
            ):
                self._bind_over_event.set()
                raise TransportNotReadyError
        else:
            self._ready = True
            self._bind_over_event.set()

    def _on_tick(self):
        """
        Tick callback. Binds the server and connects to other nodes as necessary.
        """

        try:
            self._maybe_bind()
        except TransportNotReadyError:
            pass
        self._connect_if_necessary()

    def _on_new_incoming_connection(self, conn):
        """
        Callback for connections initiated by the other side

        :param conn: connection object
        :type conn: TcpConnection
        """

        self._unknown_connections.add(conn)
        encryptor = self._RaftPysyncObject.encryptor
        if encryptor:
            conn.encryptor = encryptor
        conn.set_on_message_received_callback(
            functools.partial(self._on_incoming_message_received, conn)
        )
        conn.set_on_disconnected_callback(functools.partial(self._on_disconnected, conn))

    def _on_incoming_message_received(self, conn, message):
        """
        Callback for initial messages on incoming connections. Handles encryption, utility messages, and association of the connection with a Node.
        Once this initial setup is done, the relevant connected callback is executed, and further messages are deferred to the onMessageReceived callback.

        :param conn: connection object
        :type conn: TcpConnection
        :param message: received message
        :type message: any
        """

        if self._RaftPysyncObject.encryptor and not conn.sendRandKey:
            conn.sendRandKey = message
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
            return

        # Utility messages
        if isinstance(message, list) and self._on_utility_message(conn, message):
            return

        # At this point, message should be either a node ID (i.e. address) or 'readonly'
        node = (
            self._node_addr_to_node[message] if message in self._node_addr_to_node else None
        )

        if node is None and message != "readonly":
            conn.disconnect()
            self._unknown_connections.discard(conn)
            return

        readonly = node is None
        if readonly:
            node_id = str(self._readonly_nodes_counter)
            node = Node(node_id)
            self._readonly_nodes.add(node)
            self._readonly_nodes_counter += 1

        self._unknown_connections.discard(conn)
        self._connections[node] = conn
        conn.set_on_message_received_callback(
            functools.partial(self._on_message_received, node)
        )
        if not readonly:
            self._on_node_connected(node)
        else:
            self._on_readonly_node_connected(node)

    def _on_utility_message(self, conn, message):
        command = message[0]
        if command in self._onUtilityMessageCallbacks:
            message[0] = command.upper()
            callback = functools.partial(_utility_callback, conn=conn, args=message)
            try:
                self._onUtilityMessageCallbacks[command](message[1:], callback)
            except Exception as e:
                conn.send(str(e))
            return True

    def _should_connect(self, node):
        """
        Check whether this node should initiate a connection to another node

        :param node: the other node
        :type node: Node
        """

        return (
            isinstance(node, TCPNode)
            and node not in self._prevent_connect_nodes
            and (self._self_is_readonly_node or self._self_node.address > node.address)
        )

    def _connect_if_necessary_single(self, node: TCPNode):
        """
        Connect to a node if necessary.

        :param node: node to connect to
        :type node: Node
        """

        if (
            node in self._connections
            and self._connections[node].state != ConnectionState.DISCONNECTED
        ):
            return True
        if not self._should_connect(node):
            return False
        assert (
            node in self._connections
        )  # Since we "should connect" to this node, there should always be a connection object already in place.
        if (
            node in self._last_connect_attempt
            and monotonic_time() - self._last_connect_attempt[node]
            < self._RaftPysyncObject.conf.connection_retry_time
        ):
            return False
        self._last_connect_attempt[node] = monotonic_time()
        return self._connections[node].connect(node.ip, node.port)

    def _connect_if_necessary(self):
        """
        Connect to all nodes as necessary.
        """

        for node in self._nodes:
            self._connect_if_necessary_single(node)

    def _send_self_address(self, conn):
        if self._self_is_readonly_node:
            conn.send("readonly")
        else:
            conn.send(self._self_node.address)

    def _on_outgoing_connected(self, conn):
        """
        Callback for when a new connection from this to another node is established. Handles encryption and informs the other node which node this is.
        If encryption is disabled, this triggers the onNodeConnected callback and messages are deferred to the onMessageReceived callback.
        If encryption is enabled, the first message is handled by _on_outgoing_message_received.

        :param conn: connection object
        :type conn: TcpConnection
        """

        if self._RaftPysyncObject.encryptor:
            conn.set_on_message_received_callback(
                functools.partial(self._on_outgoing_message_received, conn)
            )  # So we can process the sendRandKey
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
        else:
            self._send_self_address(conn)
            # The onMessageReceived callback is configured in add_node already.
            self._on_node_connected(self._conn_to_node(conn))

    def _on_outgoing_message_received(self, conn, message):
        """
        Callback for receiving a message on a new outgoing connection. Used only if encryption is enabled to exchange the random keys.
        Once the key exchange is done, this triggers the onNodeConnected callback, and further messages are deferred to the onMessageReceived callback.

        :param conn: connection object
        :type conn: TcpConnection
        :param message: received message
        :type message: any
        """

        if not conn.sendRandKey:
            conn.sendRandKey = message
            self._send_self_address(conn)

        node = self._conn_to_node(conn)
        conn.set_on_message_received_callback(
            functools.partial(self._on_message_received, node)
        )
        self._on_node_connected(node)

    def _on_disconnected(self, conn):
        """
        Callback for when a connection is terminated or considered dead. Initiates a reconnect if necessary.

        :param conn: connection object
        :type conn: TcpConnection
        """

        self._unknown_connections.discard(conn)
        node = self._conn_to_node(conn)
        if node is not None:
            if node in self._nodes:
                self._on_node_disconnected(node)
                self._connect_if_necessary_single(node)
            else:
                self._readonly_nodes.discard(node)
                self._on_readonly_node_disconnected(node)

    def wait_ready(self):
        """
        Wait for the TCP transport to become ready for operation, i.e. the server to be bound.
        This method should be called from a different thread than used for the RaftPysyncObject ticks.

        :raises TransportNotReadyError: if the number of bind tries exceeds the configured limit
        """

        self._bind_over_event.wait()
        if not self._ready:
            raise TransportNotReadyError

    def add_node(self, node):
        """
        Add a node to the network

        :param node: node to add
        :type node: TCPNode
        """

        self._nodes.add(node)
        self._node_addr_to_node[node.address] = node
        if self._should_connect(node):
            conn = TcpConnection(
                poller=self._RaftPysyncObject._poller,
                timeout=self._RaftPysyncObject.conf.connection_timeout,
                send_buffer_size=self._RaftPysyncObject.conf.send_buffer_size,
                recv_buffer_size=self._RaftPysyncObject.conf.recv_buffer_size,
                keepalive=self._RaftPysyncObject.conf.tcp_keepalive,
            )
            conn.encryptor = self._RaftPysyncObject.encryptor
            conn.set_on_connected_callback(
                functools.partial(self._on_outgoing_connected, conn)
            )
            conn.set_on_message_received_callback(
                functools.partial(self._on_message_received, node)
            )
            conn.set_on_disconnected_callback(
                functools.partial(self._on_disconnected, conn)
            )
            self._connections[node] = conn

    def drop_node(self, node: TCPNode):
        """
        Drop a node from the network

        :param node: node to drop
        :type node: Node
        """

        conn: TcpConnection | None = self._connections.pop(node, None)
        if conn is not None:
            # Calling conn.disconnect() immediately triggers the onDisconnected callback if the connection isn't already disconnected, so this is necessary to prevent the automatic reconnect.
            self._prevent_connect_nodes.add(node)
            conn.disconnect()
            self._prevent_connect_nodes.remove(node)
        if isinstance(node, TCPNode):
            self._nodes.discard(node)
            self._node_addr_to_node.pop(node.address, None)
        else:
            self._readonly_nodes.discard(node)
        self._last_connect_attempt.pop(node, None)

    def send(self, node: TCPNode, message):
        """
        Send a message to a node. Returns False if the connection appears to be dead either before or after actually trying to send the message.

        :param node: target node
        :type node: Node
        :param message: message
        :param message: any
        :returns success
        :rtype bool
        """

        if (
            node not in self._connections
            or self._connections[node].state != ConnectionState.CONNECTED
        ):
            return False
        if self._send_random_sleep_duration:
            time.sleep(random.random() * self._send_random_sleep_duration)
        self._connections[node].send(message)
        if self._connections[node].state != ConnectionState.CONNECTED:
            return False
        return True

    def destroy(self):
        """
        Destroy this transport
        """

        self.set_on_message_received_callback(None)
        self.set_on_node_connected_callback(None)
        self.set_on_node_disconnected_callback(None)
        self.set_on_readonly_node_connected_callback(None)
        self.set_on_readonly_node_disconnected_callback(None)
        for node in self._nodes | self._readonly_nodes:
            self.drop_node(node)
        if self._server is not None:
            self._server.unbind()
        for conn in list(self._unknown_connections):
            conn.disconnect()
        self._unknown_connections = set()
