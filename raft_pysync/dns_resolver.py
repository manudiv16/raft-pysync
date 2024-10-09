import socket
import random
import logging
from time import monotonic as monotonic_time

logger = logging.getLogger(__name__)


class DnsCachingResolver(object):
    def __init__(self, cache_time, fail_cache_time):
        self.__cache = {}
        self.__cache_time = cache_time
        self.__fail_cache_time = fail_cache_time
        self.__preferred_addr_family = socket.AF_INET

    def set_timeouts(self, cache_time, fail_cache_time):
        self.__cache_time = cache_time
        self.__fail_cache_time = fail_cache_time

    def resolve(self, hostname):
        curr_time = monotonic_time()
        cached_time, ips = self.__cache.get(hostname, (-self.__fail_cache_time - 1, []))
        time_passed = curr_time - cached_time

        if (time_passed > self.__cache_time) or (not ips and time_passed > self.__fail_cache_time):
            if not (ips := self.__do_resolve(hostname)):
                logger.warning(f"Failed to resolve hostname: {hostname}")
                ips = self.__cache.get(hostname, (None, []))[1]  # Use previous IPs if resolution fails
            self.__cache[hostname] = (curr_time, ips)

        return random.choice(ips) if ips else None

    def set_preferred_addr_family(self, preferred_addr_family):
        addr_family_map = {
            "ipv4": socket.AF_INET,
            "ipv6": socket.AF_INET6
        }
        self.__preferred_addr_family = addr_family_map.get(preferred_addr_family, preferred_addr_family)

    def __do_resolve(self, hostname):
        try:
            address = socket.getaddrinfo(hostname, None)
            ips = {
                addr[4][0] for addr in address
                if self.__preferred_addr_family is None or addr[0] == self.__preferred_addr_family
            }
            return list(ips)  # Convert to list to avoid set serialization
        except socket.gaierror:
            logger.warning(f"Failed to resolve host {hostname}")
            return []


_g_resolver = None

def global_dns_resolver():
    global _g_resolver
    return _g_resolver if _g_resolver is not None else (_g_resolver := DnsCachingResolver(cache_time=600.0, fail_cache_time=30.0))

