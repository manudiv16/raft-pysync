from abc import ABC, abstractmethod
from dataclasses import dataclass
import socket
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import signal
# import sys

class ClusterStrategy(ABC):
    @abstractmethod
    def get_nodes(self) -> set[str]:
        """
        Abstract method to get the nodes in the cluster.
        Returns:
            set[str]: Set of node addresses.
        """
        pass

    @abstractmethod
    def polling_interval(self) -> float:
        """
        Abstract method to get the polling interval for the cluster strategy.
        Returns:
            int: Polling interval in seconds.
        """
        pass

    @abstractmethod
    def address(self) -> str:
        """
        Abstract method to get the local IP address.
        Returns:
            str: Local IP address.
        """
        pass


@dataclass
class DnsPollingStrategy(ClusterStrategy):
    """
    Cluster strategy that uses DNS polling to discover nodes in the cluster.
    """

    domain: str
    port: int
    poll_interval: int

    @staticmethod
    def __get_local_ip():
        """
        Private method to get the local IP address.
        Returns:
            str: Local IP address.
        """
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        return local_ip

    def get_nodes(self) -> set[str]:
        """
        Get the nodes in the cluster by performing DNS polling.
        Returns:
            set[str]: Set of node addresses.
        """
        try:
            local_ip = self.__get_local_ip()
            _, _, ips = socket.gethostbyname_ex(self.domain)
            return {f"{ip}:{self.port}" for ip in ips if ip != local_ip}
        except Exception:
            return set()

    def polling_interval(self):
        """
        Get the polling interval for the DNS polling strategy.
        Returns:
            int: Polling interval in seconds.
        """
        return self.poll_interval

    def address(self):
        """
        Get the local IP address.
        Returns:
            str: Local IP address.
        """
        return f"{self.__get_local_ip()}:{self.port}"


@dataclass
class StaticClusterStrategy(ClusterStrategy):
    """
    Cluster strategy that uses a static list of nodes.
    """

    nodes: set[str]
    poll_interval: float = 0.5

    def get_nodes(self) -> set[str]:
        """
        Get the nodes in the cluster.
        Returns:
            set[str]: Set of node addresses.
        """
        return self.nodes

    def polling_interval(self):
        """
        Get the polling interval for the static cluster strategy.
        Returns:
            int: Polling interval in seconds.
        """
        return self.poll_interval


# class NetworkScannerStrategy(ClusterStrategy):
#     """
#     Cluster strategy that uses network scanning to discover nodes in the cluster.
#     """
# 
#     def __init__(self, application_port, port, poll_interval=0.5):
#         self.poll_interval = poll_interval
#         self.port = port
#         self.application_port = application_port
#         self.devices = []
#         self.responses = {}
#         self._local_ip = self.__get_local_ip()
#         self.listener_thread = threading.Thread(target=self.__start_listener)
#         self.listener_thread.daemon = True
#         self.listener_thread.start()
#         signal.signal(signal.SIGINT, self.__shutdown)
#         signal.signal(signal.SIGTERM, self.__shutdown)
# 
#     def polling_interval(self):
#         """
#         Get the polling interval for the network scanner strategy.
#         Returns:
#             int: Polling interval in seconds.
#         """
#         return self.poll_interval
# 
#     def address(self):
#         """
#         Get the local IP address.
#         Returns:
#             str: Local IP address.
#         """
#         return f"{self._local_ip}:{self.application_port}"
# 
#     def __get_local_ip(self):
#         """
#         Private method to get the local IP address.
#         Returns:
#             str: Local IP address.
#         """
#         hostname = socket.gethostname()
#         local_ip = socket.gethostbyname(hostname)
#         return local_ip
# 
#     def __get_ip_range(self):
#         """
#         Private method to get the IP range for network scanning.
#         Returns:
#             str: IP range in CIDR notation.
#         """
#         ip_parts = self._local_ip.split(".")
#         ip_parts[-1] = "0"
#         return ".".join(ip_parts) + "/24"
# 
#     def __is_port_open(self, ip, port):
#         """
#         Private method to check if a port is open on a given IP address.
#         Args:
#             ip (str): IP address.
#             port (int): Port number.
#         Returns:
#             bool: True if the port is open, False otherwise.
#         """
#         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         sock.settimeout(1)
#         result = sock.connect_ex((ip, port))
#         sock.close()
#         return result == 0
# 
#     def __scan_ip(self, ip):
#         """
#         Private method to scan a single IP address for an open port.
#         Args:
#             ip (str): IP address.
#         Returns:
#             str or None: IP address if the port is open, None otherwise.
#         """
#         if self.__is_port_open(ip, self.port):
#             return ip
#         return None
# 
#     def __scan_network(self):
#         """
#         Private method to scan the network for open ports.
#         Returns:
#             list[str]: List of IP addresses with open ports.
#         """
#         ip_range = self.__get_ip_range().split("/")[0]
#         ip_base = ip_range.rsplit(".", 1)[0]
#         ip_list = [f"{ip_base}.{i}" for i in range(2, 255)]
# 
#         open_port_devices = []
#         with ThreadPoolExecutor(max_workers=100) as executor:
#             futures = [executor.submit(self.__scan_ip, ip) for ip in ip_list]
#             for future in futures:
#                 result = future.result()
#                 if result and result != self._local_ip:
#                     open_port_devices.append(result)
#         return open_port_devices
# 
#     def __communicate_with_devices(self, devices):
#         """
#         Private method to communicate with the discovered devices.
#         Args:
#             devices (list[str]): List of IP addresses.
#         Returns:
#             dict[str, str]: Dictionary of IP addresses and responses.
#         """
#         responses = {}
#         for ip in devices:
#             if ip == self._local_ip:
#                 continue
#             try:
#                 with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                     s.connect((ip, self.port))
#                     message = f"ping"
#                     s.sendall(message.encode())
#                     data = s.recv(1024)
#                     responses[ip] = data.decode()
#             except Exception:
#                 pass
#         responses = {f"{k}:{self.application_port}" for k in responses.keys()}
#         return responses
# 
#     def __handle_client(self, client_socket):
#         """
#         Private method to handle client connections.
#         Args:
#             client_socket (socket.socket): Client socket object.
#         """
#         _ = client_socket.recv(1024)
#         response = f"pong"
#         client_socket.send(response.encode())
#         client_socket.close()
# 
#     def __shutdown(self):
#         """
#         Private method to shutdown the network scanner strategy.
#         """
#         self.running = False
#         self.listener_thread.join()
#         sys.exit(0)
# 
#     def __start_listener(self):
#         """
#         Private method to start the listener for incoming connections.
#         """
#         server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         server.bind(("", self.port))
#         server.listen(5)
# 
#         while True:
#             client_socket, _ = server.accept()
#             client_handler = threading.Thread(
#                 target=self.__handle_client, args=(client_socket,)
#             )
#             client_handler.start()
# 
#     def get_nodes(self) -> set[str]:
#         """
#         Get the nodes in the cluster by performing network scanning.
#         Returns:
#             set[str]: Set of node addresses.
#         """
#         self.devices = self.__scan_network()
#         self.responses = self.__communicate_with_devices(self.devices)
#         return self.responses
