from .dns_resolver import global_dns_resolver


class Node:
    """
    A representation of any node in the network.

    The ID must uniquely identify a node. Node objects with the same ID will be treated as equal, i.e. as representing the same node.
    """

    def __init__(self, id, **kwargs):
        """
        Initialise the Node; id must be immutable, hashable, and unique.

        :param id: unique, immutable, hashable ID of a node
        :type id: any
        :param **kwargs: any further information that should be kept about this node
        """

        self._id = id
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __setattr__(self, name, value):
        if name == "id":
            raise AttributeError("Node id is not mutable")
        super(Node, self).__setattr__(name, value)

    def __eq__(self, other):
        return isinstance(other, Node) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return self.id

    def __repr__(self):
        v = vars(self)
        attrs = ", ".join(f"{key} = {repr(value)}" for key, value in v.items() if key != "_id")
        return f"{type(self).__name__}({repr(self.id)}{', ' + attrs if attrs else ''})"

    def destroy(self):
        pass

    @property
    def id(self):
        return self._id


class TCPNode(Node):
    """
    A node intended for communication over TCP/IP. Its id is the network address (host:port).
    """

    def __init__(self, address: str, **kwargs):
        """
        Initialise the TCPNode

        :param address: network address of the node in the format 'host:port'
        :type address: str
        :param **kwargs: any further information that should be kept about this node
        """

        super(TCPNode, self).__init__(address, **kwargs)
        self.__address = address
        self.__host, port = address.rsplit(":", 1)
        self.__port = int(port)

    @property
    def address(self) -> str:
        return self.__address

    @property
    def host(self) -> str:
        return self.__host

    @property
    def port(self) -> int:
        return self.__port

    @property
    def ip(self) -> str:
        return global_dns_resolver().resolve(self.__host)

    def __repr__(self):
        v = vars(self)
        filtered = {"_id", "_TCPNode__address", "_TCPNode__host", "_TCPNode__port", "_TCPNode__ip"}
        formatted = [f"{key} = {repr(value)}" for key, value in v.items() if key not in filtered]
        return f"{type(self).__name__}({repr(self.id)}{', ' + ', '.join(formatted) if formatted else ''})"

