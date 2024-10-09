import time
import random
import os
import sys
import threading
import weakref
import collections
import functools
import struct
import logging
import queue as Queue
import raft_pysync.pickle as pickle

from .dns_resolver import global_dns_resolver
from .poller import createPoller
from .pipe_notifier import PipeNotifier

try:

    PIPE_NOTIFIER_ENABLED = True
except ImportError:
    PIPE_NOTIFIER_ENABLED = False

from .serializer import Serializer, SerializerState
from .node import Node, TCPNode
from .transport import TCPTransport, TransportNotReadyError, Transport
from .journal import create_journal
from .config import RaftPysyncObjectConf, FailReason
from .encryptor import HAS_CRYPTO, get_encryptor
from .version import VERSION
from .cluster_strategy import ClusterStrategy
from time import monotonic as monotonic_time
logger = logging.getLogger(__name__)

from enum import Enum
class RaftState(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class CommandType(Enum):
    REGULAR = 0
    NO_OP = 1
    MEMBERSHIP = 2
    VERSION = 3


_wchar = functools.partial(struct.pack, "B")

class RaftPysyncObjectConsumer:
    def __init__(self):
        self._RaftPysyncObject: RaftPysyncObject | None = None
        self.__properties = set()
        for key in self.__dict__:
            self.__properties.add(key)

    def destroy(self):
        self._RaftPysyncObject = None

    def _serialize(self):
        return dict(
            [(k, v) for k, v in self.__dict__.items() if k not in self.__properties]
        )

    def _deserialize(self, data: dict):
        for k, v in data.items():
            self.__dict__[k] = v

from abc import ABC, abstractmethod

class ConsumerHelper(ABC):
    @abstractmethod
    def consumer(self):
        pass
    @abstractmethod
    def destroy(self):
        pass

    @abstractmethod
    def _serialize(self):
        pass

    @abstractmethod
    def _deserialize(self, data: dict):
        pass


type Consumer = ConsumerHelper | RaftPysyncObjectConsumer

class RaftPysyncObjectException(Exception):
    def __init__(self, error_code, *args, **kwargs):
        Exception.__init__(self, *args)
        self.errorCode = error_code


class RaftPysyncObjectExceptionWrongVer(RaftPysyncObjectException):
    def __init__(self, ver):
        RaftPysyncObjectException.__init__(self, "wrongVer")
        self.ver = ver




def parse_change_cluster_request(command):
    command_type = ord(command[:1])
    if command_type != CommandType.MEMBERSHIP.value:
        return None
    return pickle.loads(command[1:])


class RaftPysyncObject:
    def __init__(
        self,
        self_node: TCPNode | str | None = None,
        other_nodes: set[ str| TCPNode ] = set(),
        conf = None,
        consumers = None,
        node_class = TCPNode,
        transport_class = TCPTransport,
        clustering_strategy: ClusterStrategy | None = None,
    ):
        """
        Main SyncObj class, you should inherit your own class from it.

        :param selfNode: object representing the self-node or address of the current node server 'host:port'
        :type selfNode: Node or str
        :param other_nodes: objects representing the other nodes or addresses of partner nodes ['host1:port1', 'host2:port2', ...]
        :type other_nodes: iterable of Node or iterable of str
        :param conf: configuration object
        :type conf: SyncObjConf
        :param consumers: objects to be replicated
        :type consumers: list of SyncObjConsumer inherited objects
        :param node_class: class used for representation of nodes
        :type node_class: class
        :param transport: transport object; if None, transportClass is used to initialise such an object
        :type transport: Transport or None
        :param transport_class: the Transport subclass to be used for transferring messages to and from other nodes
        :type transport_class: class
        :param clustering_strategy: the strategy to be used for auto-discovery of nodes
        :type clustering_strategy: class
        """

        if conf is None:
            self.__conf = RaftPysyncObjectConf()
        else:
            self.__conf = conf

        self.__cluster_strategy = clustering_strategy

        self.__conf.validate()

        if self.__conf.password is not None:
            if not HAS_CRYPTO:
                raise ImportError("Please install 'cryptography' module")
            self.__encryptor = get_encryptor(self.__conf.password)
        else:
            self.__encryptor = None

        consumers: list[Consumer] = consumers or []
        new_consumers = []
        for c in consumers:
            if not isinstance(c, RaftPysyncObjectConsumer) and ConsumerHelper:
                c = c.consumer()
            if not isinstance(c, RaftPysyncObjectConsumer):
                raise RaftPysyncObjectException(
                    "Consumers must be inherited from SyncObjConsumer"
                )
            new_consumers.append(c)
        consumers = new_consumers

        self.__consumers = consumers
        if self.__cluster_strategy and not self_node:
            self_node: str = self.__cluster_strategy.address()
        orig_self_node = self_node
        if not self_node:
            raise RaftPysyncObjectException("selfNode is not set")
        self.__self_node = self_node if isinstance(self_node, TCPNode) else node_class(self_node)
        self.__other_nodes = set()  # set of Node
        for other_node in other_nodes:
            if other_node == orig_self_node:
                continue
            if not isinstance(other_node, TCPNode):
                other_node = node_class(other_node)
            self.__other_nodes.add(other_node)
        self.__readonly_nodes = set()  # set of Node
        self.__connected_nodes = set()  # set of Node
        self.__node_class  = node_class
        self.__raft_state = RaftState.FOLLOWER
        self.__raft_current_term = 0
        self.__voted_for_node_id = None
        self.__votes_count = 0
        self.__raft_leader = None
        self.__raft_election_deadline = monotonic_time() + self.__generate_raft_timeout()
        self.__raft_log = create_journal(self.__conf.journal_file)
        if len(self.__raft_log) == 0:
            self.__raft_log.add(_wchar(CommandType.NO_OP.value), 1, self.__raft_current_term)
        self.__raft_commit_index = self.__raft_log.getRaftCommitIndex()
        self.__raft_last_applied = 1
        self.__raft_next_index = {}
        self.__last_response_time = {}
        self.__raft_match_index = {}
        self.__last_serialized_time = monotonic_time()
        self.__last_serialized_entry = None
        self.__force_log_compaction = False
        self.__leader_commit_index = None
        self.__on_ready_called = False
        self.__change_cluster_i_dx = None
        self.__noop_i_dx = None
        self.__destroying = False
        self.__recv_transmission = ""

        self.__on_tick_callbacks = []
        self.__on_tick_callbacks_lock = threading.Lock()

        self.__start_time = monotonic_time()
        self.__num_one_second_dumps = 0
        global_dns_resolver().set_timeouts(
            self.__conf.dns_cache_time, self.__conf.dns_fail_cache_time
        )
        global_dns_resolver().set_preferred_addr_family(self.__conf.preferred_addr_type)
        self.__serializer = Serializer(
            self.__conf.full_dump_file,
            self.__conf.log_compaction_batch_size,
            self.__conf.use_fork,
            self.__conf.serializer,
            self.__conf.deserializer,
            self.__conf.serialize_checker,
        )
        self._poller = createPoller(self.__conf.poller_type)

        self.__transport: Transport = transport_class(self, self.__self_node, self.__other_nodes)

        self.__transport.set_on_node_connected_callback( self.__on_node_connected )
        self.__transport.set_on_node_disconnected_callback(self.__on_node_disconnected)
        self.__transport.set_on_message_received_callback(self.__on_message_received)
        self.__transport.set_on_readonly_node_connected_callback(
            self.__on_readonly_node_connected
        )
        self.__transport.set_on_readonly_node_disconnected_callback(
            self.__on_readonly_node_disconnected
        )
        self.__transport.set_on_utility_message_callback("status", self._get_status)
        self.__transport.set_on_utility_message_callback("add", self._add_node_to_cluster)
        self.__transport.set_on_utility_message_callback(
            "remove", self._remove_node_from_cluster
        )
        self.__transport.set_on_utility_message_callback(
            "set_version", self._set_code_version
        )

        self._method_to_id = {}
        self._id_to_method = {}
        self._id_to_consumer = {}

        methods = [
            m
            for m in dir(self)
            if callable(getattr(self, m))
            and getattr(getattr(self, m), "replicated", False)
            and m != getattr(getattr(self, m), "origName")
        ]

        curr_method_id = 0
        self.__self_code_version = 0
        self.__current_version_func_names = {}

        methods_to_enumerate = []
        for method in methods:

            ver = getattr(getattr(self, method), "ver")
            methods_to_enumerate.append((ver, 0, method, self))

        for consumerNum, consumer in enumerate(consumers):
            consumer_methods = [
                m
                for m in dir(consumer)
                if callable(getattr(consumer, m))
                and getattr(getattr(consumer, m), "replicated", False)
                and m != getattr(getattr(consumer, m), "origName")
            ]
            for method in consumer_methods:
                ver = getattr(getattr(consumer, method), "ver")
                methods_to_enumerate.append((ver, consumerNum + 1, method, consumer))
        for ver, _, method, obj in sorted(methods_to_enumerate):
            self.__self_code_version = max(self.__self_code_version, ver)
            if obj is self:
                self._method_to_id[method] = curr_method_id
            else:
                self._method_to_id[(id(obj), method)] = curr_method_id
            self._id_to_method[curr_method_id] = getattr(obj, method)
            curr_method_id += 1

        self.__on_set_code_version(0)

        self.__thread = None
        self.__main_thread = None
        self.__cluster_thread = None
        self.__initialised = None
        self.__commands_queue = Queue.Queue(maxsize=self.__conf.commands_queue_size)
        if not self.__conf.append_entries_use_batch and PIPE_NOTIFIER_ENABLED:
            self.__pipe_notifier = PipeNotifier(self._poller)
        self.__need_load_dump_file = True

        self.__last_readonly_check = 0
        self.__new_append_entries_time = 0

        self.__commands_waiting_commit = collections.defaultdict(
            list
        )  # logID => [(termID, callback), ...]
        self.__commands_local_counter = 0
        self.__commands_waiting_reply = {}  # commandLocalCounter => callback

        self.__properties = set()
        for key in self.__dict__:
            self.__properties.add(key)

        self.__enabled_code_version = 0

        if self.__conf.auto_tick or self.__cluster_strategy:
            self.__main_thread = threading.current_thread()
            self.__initialised = threading.Event()
            if self.__cluster_strategy:
                self.__cluster_thread = threading.Thread(
                    target=RaftPysyncObject._clustering_polling_thread,
                    args=(weakref.proxy(self),),
                )
                self.__cluster_thread.start()

            if self.__conf.auto_tick:
                self.__thread = threading.Thread(
                    target=RaftPysyncObject._auto_tick_thread, args=(weakref.proxy(self),)
                )
                self.__thread.start()

            self.__initialised.wait()

        else:
            try:
                while not self.__transport.ready:
                    self.__transport.try_get_ready()
            except TransportNotReadyError:
                logger.exception("failed to perform initialization")
                raise RaftPysyncObjectException("BindError")  # Backwards compatibility



# -------------------------------- @property --------------------------------


    @property
    def self_node(self):
        """
        :rtype: Node
        """
        return self.__self_node

    @property
    def other_nodes(self):
        """
        :rtype: set of Node
        """
        return self.__other_nodes.copy()

    @property
    def readonly_nodes(self):
        """
        :rtype: set of Node
        """
        return self.__readonly_nodes.copy()

    @property
    def raft_last_applied(self):
        """
        :rtype: int
        """
        return self.__raft_last_applied

    @property
    def raft_commit_index(self):
        """
        :rtype: int
        """
        return self.__raft_commit_index

    @property
    def raft_current_term(self):
        """
        :rtype: int
        """
        return self.__raft_current_term

    @property
    def has_quorum(self):
        """
        Does the cluster have a quorum according to this node

        :rtype: bool
        """

        nodes = self.__other_nodes
        node_count = len(nodes)
        # Get number of connected nodes that participate in cluster quorum
        connected_count = len(nodes.intersection(self.__connected_nodes))

        if self.__self_node is not None:
            # This node participates in cluster quorum
            connected_count += 1
            node_count += 1

        return connected_count > node_count / 2

    @property
    def conf(self):
        return self.__conf

    @property
    def encryptor(self):
        return self.__encryptor

    @property
    def poller(self):
        return self._poller

    @property
    def method_to_id(self):
        return self._method_to_id


# -----------------------------------------------------------------------------


    def destroy(self):
        """
        Correctly destroy RaftPysyncObject. Stop autoTickThread, close connections, etc.
        """
        if self.__conf.auto_tick:
            self.__destroying = True
        else:
            self._do_destroy()

    def wait_ready(self):
        """
        Waits until the transport is ready for operation.

        :raises TransportNotReadyError: if the transport fails to get ready
        """
        self.__transport.wait_ready()

    def wait_bound(self):
        """
        Waits until initialized (bound port).
        If success - just returns.
        If failed to initialized after conf.maxBindRetries - raise RaftPysyncObjectException.
        """
        try:
            self.__transport.wait_ready()
        except TransportNotReadyError:
            raise RaftPysyncObjectException("BindError")
        if not self.__transport.ready:
            raise RaftPysyncObjectException("BindError")

    def _destroy(self):
        self.destroy()

    def _do_destroy(self):
        self.__transport.destroy()
        for consumer in self.__consumers:
            consumer.destroy()
        self.__raft_log._destroy()

    def get_code_version(self):
        return self.__enabled_code_version

    def set_code_version(self, new_version, callback=None):
        """Switch to a new code version on all cluster nodes. You
        should ensure that cluster nodes are updated, otherwise they
        won't be able to apply commands.

        :param new_version: new code version
        :type int
        :param callback: will be called on success or fail
        :type callback: function(`FAIL_REASON <#raft_pysync.FAIL_REASON>`_, None)
        """
        assert isinstance(new_version, int)
        if new_version > self.__self_code_version:
            raise Exception(
                "wrong version, current version is %d, requested version is %d"
                % (self.__self_code_version, new_version)
            )
        if new_version < self.__enabled_code_version:
            raise Exception(
                "wrong version, enabled version is %d, requested version is %d"
                % (self.__enabled_code_version, new_version)
            )
        self.apply_command(pickle.dumps(new_version), callback, CommandType.VERSION.value)

    def add_node_to_cluster(self, node: TCPNode | str, callback=None):
        """Add single node to cluster (dynamic membership changes). Async.
        You should wait until node successfully added before adding
        next node.

        :param node: node object or 'nodeHost:nodePort'
        :type node: Node | str
        :param callback: will be called on success or fail
        :type callback: function(`FAIL_REASON <#raft_pysync.FAIL_REASON>`_, None)
        """
        if not self.__conf.dynamic_membership_change:
            raise Exception("dynamicMembershipChange is disabled")
        if not isinstance(node, TCPNode):
            node = self.__node_class(node)
        self.apply_command(
            pickle.dumps(["add", node.id, node]), callback, CommandType.MEMBERSHIP.value
        )

    def remove_node_from_cluster(self, node: TCPNode | str, callback=None):
        """Remove single node from cluster (dynamic membership changes). Async.
        You should wait until node successfully added before adding
        next node.

        :param node: node object or 'nodeHost:nodePort'
        :type node: Node | str
        :param callback: will be called on success or fail
        :type callback: function(`FAIL_REASON <#raft_pysync.FAIL_REASON>`_, None)
        """
        if not self.__conf.dynamic_membership_change:
            raise Exception("dynamicMembershipChange is disabled")
        if not isinstance(node, TCPNode):
            node = self.__node_class(node)
        self.apply_command(
            pickle.dumps(["rem", node.id, node]), callback, CommandType.MEMBERSHIP.value
        )

    def _set_code_version(self, args, callback):
        self.set_code_version(args[0], callback)

    def _add_node_to_cluster(self, args, callback):
        self.add_node_to_cluster(args[0], callback)

    def _remove_node_from_cluster(self, args, callback):
        node = args[0]
        if not self.__self_node:
            raise Exception("self_node is not set")
        if node == self.__self_node.id:
            callback(None, FailReason.REQUEST_DENIED)
        else:
            self.remove_node_from_cluster(node, callback)

    def __on_set_code_version(self, new_version):
        methods = [
            m
            for m in dir(self)
            if callable(getattr(self, m))
            and getattr(getattr(self, m), "replicated", False)
            and m != getattr(getattr(self, m), "origName")
        ]
        self.__current_version_func_names = {}

        func_versions = collections.defaultdict(set)
        for method in methods:
            ver = getattr(getattr(self, method), "ver")
            orig_func_name = getattr(getattr(self, method), "origName")
            func_versions[orig_func_name].add(ver)

        for consumer in self.__consumers:
            consumer_id = id(consumer)
            consumer_methods = [
                m
                for m in dir(consumer)
                if callable(getattr(consumer, m))
                and getattr(getattr(consumer, m), "replicated", False)
            ]
            for method in consumer_methods:
                ver = getattr(getattr(consumer, method), "ver")
                orig_func_name = getattr(getattr(consumer, method), "origName")
                func_versions[(consumer_id, orig_func_name)].add(ver)

        for funcName, versions in func_versions.items():
            versions = sorted(list(versions))
            for v in versions:
                if v > new_version:
                    break
                real_func_name: str = funcName[1] if isinstance(funcName, tuple) else funcName
                self.__current_version_func_names[funcName] = real_func_name + "_v" + str(v)

    def get_func_name(self, func_name):
        return self.__current_version_func_names[func_name]

    def apply_command(self, command, callback, command_type  =None):
        try:
            if command_type is None:
                self.__commands_queue.put_nowait((command, callback))
            else:
                self.__commands_queue.put_nowait(
                    (_wchar(command_type.value) + command, callback)
                )
            if not self.__conf.append_entries_use_batch and PIPE_NOTIFIER_ENABLED:
                self.__pipe_notifier.notify()
        except Queue.Full:
            self.__call_err_callback(FailReason.QUEUE_FULL, callback)

    def _check_commands_to_apply(self):
        start_time = monotonic_time()

        while monotonic_time() - start_time < self.__conf.append_entries_period:
            if self.__raft_leader is None and self.__conf.commands_wait_leader:
                break
            try:
                command, callback = self.__commands_queue.get_nowait()
            except Queue.Empty:
                break
            request_node, request_id = None, None
            if isinstance(callback, tuple):
                request_node, request_id = callback

            if self.__raft_state == RaftState.LEADER:
                idx, term = self.__get_current_log_index() + 1, self.__raft_current_term

                if self.__conf.dynamic_membership_change:
                    change_cluster_request = parse_change_cluster_request(command)
                else:
                    change_cluster_request = None

                if change_cluster_request is None or self.__change_cluster(
                    change_cluster_request
                ):

                    self.__raft_log.add(command, idx, term)

                    if request_node is None:
                        if callback is not None:
                            self.__commands_waiting_commit[idx].append((term, callback))
                    else:
                        self.__transport.send(
                            request_node,
                            {
                                "type": "apply_command_response",
                                "request_id": request_id,
                                "log_idx": idx,
                                "log_term": term,
                            },
                        )
                    if not self.__conf.append_entries_use_batch:
                        self.__send_append_entries()
                else:

                    if request_node is None:
                        if callback is not None:
                            callback(None, FailReason.REQUEST_DENIED)
                    else:
                        self.__transport.send(
                            request_node,
                            {
                                "type": "apply_command_response",
                                "request_id": request_id,
                                "error": FailReason.REQUEST_DENIED,
                            },
                        )

            elif self.__raft_leader is not None:
                if request_node is None:
                    message = {
                        "type": "apply_command",
                        "command": command,
                    }

                    if callback is not None:
                        self.__commands_local_counter += 1
                        self.__commands_waiting_reply[self.__commands_local_counter] = (
                            callback
                        )
                        message["request_id"] = self.__commands_local_counter

                    self.__transport.send(self.__raft_leader, message)
                else:
                    self.__transport.send(
                        request_node,
                        {
                            "type": "apply_command_response",
                            "request_id": request_id,
                            "error": FailReason.NOT_LEADER,
                        },
                    )
            else:
                self.__call_err_callback(FailReason.MISSING_LEADER, callback)

    def _clustering_polling_thread(self):
        if not self.__cluster_strategy:
            raise Exception("Cluster strategy is not set for this object instance")
        while True:
            if not self.__main_thread:
                raise Exception("Main thread is not set")
            if not self.__main_thread.is_alive():
                break
            if self.__destroying:
                self._do_destroy()
                break
            time.sleep(self.__cluster_strategy.polling_interval())
            discovery_nodes = {
                self.__node_class(node) for node in self.__cluster_strategy.get_nodes()
            }
            logger.debug(f"New nodes: {discovery_nodes}")
            logger.debug(f"Old nodes: {self.__other_nodes}")
            added_nodes = discovery_nodes.difference(self.__other_nodes)
            logger.debug(f"Added nodes: {added_nodes}")
            for node in added_nodes:
                self.add_node_to_cluster(node)

    def _auto_tick_thread(self):
        try:
            self.__transport.try_get_ready()
        except TransportNotReadyError:
            logger.exception("failed to perform initialization")
            return
        finally:
            if self.__initialised:
                self.__initialised.set()
        time.sleep(0.1)
        try:
            while True:
                if not self.__main_thread:
                    raise Exception("Main thread is not set")
                if not self.__main_thread.is_alive():
                    break
                if self.__destroying:
                    self._do_destroy()
                    break
                self._on_tick(self.__conf.auto_tick_period)
        except ReferenceError:
            pass

    def do_tick(self, time_to_wait=0.0):
        """Performs single tick. Should be called manually if `autoTick <#raft_pysync.RaftPysyncObjectConf.autoTick>`_ disabled

        :param time_to_wait: max time to wait for next tick. If zero - perform single tick without waiting for new events.
            Otherwise - wait for new socket event and return.
        :type time_to_wait: float
        """
        assert not self.__conf.auto_tick
        self._on_tick(time_to_wait)

    def _on_tick(self, time_to_wait=0.0):
        if not self.__transport.ready:
            try:
                self.__transport.try_get_ready()
            except TransportNotReadyError:
                # Implicitly handled in the 'if not self.__transport.ready' below
                pass

        if not self.__transport.ready:
            time.sleep(time_to_wait)
            self.__apply_log_entries()
            return

        if self.__need_load_dump_file:
            if self.__conf.full_dump_file is not None and os.path.isfile(
                self.__conf.full_dump_file
            ):
                self.__load_dump_file(clear_journal=False)
            self.__need_load_dump_file = False

        work_time = monotonic_time() - self.__start_time
        if work_time > self.__num_one_second_dumps:
            self.__num_one_second_dumps += 1
            self.__raft_log.onOneSecondTimer()

        if (
            self.__raft_state in (RaftState.FOLLOWER, RaftState.CANDIDATE)
            and self.__self_node is not None
        ):
            if (
                self.__raft_election_deadline < monotonic_time()
                and self.__connected_to_anyone()
            ):
                self.__raft_election_deadline = (
                        monotonic_time() + self.__generate_raft_timeout()
                )
                self.__raft_leader = None
                self.__set_state(RaftState.CANDIDATE)
                self.__raft_current_term += 1
                self.__voted_for_node_id = self.__self_node.id
                self.__votes_count = 1
                logger.debug("start new election")
                for node in self.__other_nodes:
                    self.__transport.send(
                        node,
                        {
                            "type": "request_vote",
                            "term": self.__raft_current_term,
                            "last_log_index": self.__get_current_log_index(),
                            "last_log_term": self.__get_current_log_term(),
                        },
                    )
                self.__on_leader_changed()
                if self.__votes_count > (len(self.__other_nodes) + 1) / 2:
                    self.__on_become_leader()

        if self.__raft_state == RaftState.LEADER:

            commit_idx = self.__raft_commit_index
            next_commit_idx = self.__raft_commit_index

            while commit_idx < self.__get_current_log_index():
                commit_idx += 1
                count = 1
                for node in self.__other_nodes:
                    if self.__raft_match_index[node] >= commit_idx:
                        count += 1
                if count <= (len(self.__other_nodes) + 1) / 2:
                    break
                entries = self.__get_entries(commit_idx, 1)
                if not entries:
                    continue
                commit_term = entries[0][2]
                if commit_term != self.__raft_current_term:
                    continue
                next_commit_idx = commit_idx

            if self.__raft_commit_index != next_commit_idx:
                self.__raft_commit_index = next_commit_idx
                self.__raft_log.setRaftCommitIndex(self.__raft_commit_index)

            self.__leader_commit_index = self.__raft_commit_index
            deadline = monotonic_time() - self.__conf.leader_fallback_timeout
            count = 1
            for node in self.__other_nodes:
                if self.__last_response_time[node] > deadline:
                    count += 1
            if count <= (len(self.__other_nodes) + 1) / 2:
                self.__set_state(RaftState.FOLLOWER)
                self.__raft_leader = None

        need_send_append_entries = self.__apply_log_entries()

        if self.__raft_state == RaftState.LEADER:
            if monotonic_time() > self.__new_append_entries_time or need_send_append_entries:
                self.__send_append_entries()

        if (
            not self.__on_ready_called
            and self.__raft_last_applied == self.__leader_commit_index
        ):
            if self.__conf.on_ready:
                self.__conf.on_ready()
            self.__on_ready_called = True

        self._check_commands_to_apply()
        self.__try_log_compaction()

        with self.__on_tick_callbacks_lock:
            for callback in self.__on_tick_callbacks:
                callback()

        self._poller.poll(time_to_wait)

    def __apply_log_entries(self):
        need_send_append_entries = False

        if self.__raft_commit_index > self.__raft_last_applied:
            count = self.__raft_commit_index - self.__raft_last_applied
            entries = self.__get_entries(self.__raft_last_applied + 1, count)
            for entry in entries:
                try:
                    current_term_id = entry[2]
                    subscribers = self.__commands_waiting_commit.pop(entry[1], [])
                    res = self.__do_apply_command(entry[0])
                    for subscribeTermID, callback in subscribers:
                        if subscribeTermID == current_term_id:
                            callback(res, FailReason.SUCCESS)
                        else:
                            callback(None, FailReason.DISCARDED)

                    self.__raft_last_applied += 1
                except RaftPysyncObjectExceptionWrongVer as e:
                    logger.error(
                        "request to switch to unsupported code version (self version:"
                        " %d, requested version: %d)" % (self.__self_code_version, e.ver)
                    )

            if not self.__conf.append_entries_use_batch:
                need_send_append_entries = True

        return need_send_append_entries

    def add_on_tick_callback(self, callback):
        with self.__on_tick_callbacks_lock:
            self.__on_tick_callbacks.append(callback)

    def remove_on_tick_callback(self, callback):
        with self.__on_tick_callbacks_lock:
            try:
                self.__on_tick_callbacks.remove(callback)
            except ValueError:
                # callback not in list, ignore
                pass

    def is_node_connected(self, node):
        """
        Checks if the given node is connected
        :param node: node to check
        :type node: Node
        :rtype: bool
        """
        return node in self.__connected_nodes



    def get_status(self):
        """Dumps different debug info about cluster to dict and return it"""

        status = {"version": VERSION, "revision": "deprecated", "self": self.self_node, "state": self.__raft_state,
                  "leader": self.__raft_leader, "has_quorum": self.has_quorum,
                  "partner_nodes_count": len(self.__other_nodes)}
        other_nodes = self.transform_node(self.__other_nodes)

        for node in other_nodes:
            status["partner_node_status_server_" + node.id] = (
                2 if self.is_node_connected(node) else 0
            )
        status["readonly_nodes_count"] = len(self.__readonly_nodes)
        for node in self.__readonly_nodes:
            status["readonly_node_status_server_" + node.id] = (
                2 if self.is_node_connected(node) else 0
            )
        status["log_len"] = len(self.__raft_log)
        status["last_applied"] = self.raft_last_applied
        status["commit_idx"] = self.raft_commit_index
        status["raft_term"] = self.raft_current_term
        status["next_node_idx_count"] = len(self.__raft_next_index)
        for node, idx in self.__raft_next_index.items():
            status["next_node_idx_server_" + node.id] = idx
        status["match_idx_count"] = len(self.__raft_match_index)
        for node, idx in self.__raft_match_index.items():
            status["match_idx_server_" + node.id] = idx
        status["leader_commit_idx"] = self.__leader_commit_index
        status["uptime"] = int(monotonic_time() - self.__start_time)
        status["self_code_version"] = self.__self_code_version
        status["enabled_code_version"] = self.__enabled_code_version
        return status

    def _get_status(self, args, callback):
        callback(self.get_status(), None)

    def print_status(self):
        """Dumps different debug info about cluster to default logger"""
        status = self.get_status()
        for k, v in status.items():
            logger.info("%s: %s" % (str(k), str(v)))

    def _print_status(self):
        self.print_status()

    def force_log_compaction(self):
        """Force to start log compaction (without waiting required time or required number of entries)"""
        self.__force_log_compaction = True

    def _force_log_compaction(self):
        self.force_log_compaction()

    def __do_apply_command(self, command):
        command_type = ord(command[:1])
        print(command_type)
        # Skip no-op and membership change commands
        if command_type == CommandType.VERSION.value:
            print("version")
            ver = pickle.loads(command[1:])
            if self.__self_code_version < ver:
                raise RaftPysyncObjectExceptionWrongVer(ver)
            old_ver = self.__enabled_code_version
            self.__enabled_code_version = ver
            callback = self.__conf.on_code_version_changed
            self.__on_set_code_version(ver)
            if callback is not None:
                callback(old_ver, ver)
            return

        #  This is required only after node restarts and apply journal
        # for normal case it is already done earlier and calls will be ignored
        cluster_change_request = parse_change_cluster_request(command)
        if cluster_change_request is not None:
            self.__do_change_cluster(cluster_change_request)
            return

        if command_type != CommandType.REGULAR.value:
            return
        command = pickle.loads(command[1:])
        args = []
        kwargs = {
            "_doApply": True,
        }
        if not isinstance(command, tuple):
            func_id = command
        elif len(command) == 2:
            func_id, args = command
        else:
            func_id, args, new_kw_args = command
            kwargs.update(new_kw_args)

        return self._id_to_method[func_id](*args, **kwargs)

    def __on_message_received(self, node, message):

        if message["type"] == "request_vote" and self.__self_node is not None:

            if message["term"] > self.__raft_current_term:
                self.__raft_current_term = message["term"]
                self.__voted_for_node_id = None
                self.__set_state(RaftState.FOLLOWER)
                self.__raft_leader = None

            if self.__raft_state in (RaftState.FOLLOWER, RaftState.CANDIDATE):
                last_log_term = message["last_log_term"]
                last_log_idx = message["last_log_index"]
                if message["term"] >= self.__raft_current_term:
                    if last_log_term < self.__get_current_log_term():
                        return
                    if (
                        last_log_term == self.__get_current_log_term()
                        and last_log_idx < self.__get_current_log_index()
                    ):
                        return
                    if self.__voted_for_node_id is not None:
                        return

                    self.__voted_for_node_id = node.id

                    self.__raft_election_deadline = (
                            monotonic_time() + self.__generate_raft_timeout()
                    )
                    self.__transport.send(
                        node,
                        {
                            "type": "response_vote",
                            "term": message["term"],
                        },
                    )

        if (
            message["type"] == "append_entries"
            and message["term"] >= self.__raft_current_term
        ):
            self.__raft_election_deadline = monotonic_time() + self.__generate_raft_timeout()
            if self.__raft_leader != node:
                self.__on_leader_changed()
            self.__raft_leader = node
            if message["term"] > self.__raft_current_term:
                self.__raft_current_term = message["term"]
                self.__voted_for_node_id = None
            self.__set_state(RaftState.FOLLOWER)
            new_entries = message.get("entries", [])
            serialized = message.get("serialized", None)
            self.__leader_commit_index = leaderCommitIndex = message["commit_index"]

            # Regular append entries
            if "prev_log_idx" in message:
                transmission = message.get("transmission", None)
                if transmission is not None:
                    if transmission == "start":
                        self.__recv_transmission = message["data"]
                        self.__sendNextNodeIdx(node, success=False, reset=False)
                        return
                    elif transmission == "process":
                        self.__recv_transmission += message["data"]
                        self.__sendNextNodeIdx(node, success=False, reset=False)
                        return
                    elif transmission == "finish":
                        self.__recv_transmission += message["data"]
                        new_entries = [pickle.loads(self.__recv_transmission)]
                        self.__recv_transmission = ""
                    else:
                        raise Exception("Wrong transmission type")

                prev_log_idx = message["prev_log_idx"]
                prev_log_term = message["prev_log_term"]
                prev_entries = self.__get_entries(prev_log_idx)
                if not prev_entries:
                    self.__sendNextNodeIdx(node, success=False, reset=True)
                    return
                if prev_entries[0][2] != prev_log_term:
                    self.__sendNextNodeIdx(
                        node, nextNodeIdx=prev_log_idx, success=False, reset=True
                    )
                    return
                if len(prev_entries) > 1:
                    # rollback cluster changes
                    if self.__conf.dynamic_membership_change:
                        for entry in reversed(prev_entries[1:]):
                            cluster_change_request = parse_change_cluster_request(
                                entry[0]
                            )
                            if cluster_change_request is not None:
                                self.__do_change_cluster(
                                    cluster_change_request, reverse=True
                                )

                    self.__delete_entries_from(prev_log_idx + 1)
                for entry in new_entries:
                    self.__raft_log.add(*entry)

                # apply cluster changes
                if self.__conf.dynamic_membership_change:
                    for entry in new_entries:
                        cluster_change_request = parse_change_cluster_request(
                            entry[0]
                        )
                        if cluster_change_request is not None:
                            self.__do_change_cluster(cluster_change_request)

                next_node_idx = prev_log_idx + 1
                if new_entries:
                    next_node_idx = new_entries[-1][1] + 1

                self.__sendNextNodeIdx(node, nextNodeIdx=next_node_idx, success=True)

            # Install snapshot
            elif serialized is not None:
                if self.__serializer.setTransmissionData(serialized):
                    self.__load_dump_file(clear_journal=True)
                    self.__sendNextNodeIdx(node, success=True)

            if leaderCommitIndex > self.__raft_commit_index:
                self.__raft_commit_index = min(
                    leaderCommitIndex, self.__get_current_log_index()
                )

            self.__raft_log.setRaftCommitIndex(self.__raft_commit_index)

        if message["type"] == "apply_command":
            if "request_id" in message:
                self.apply_command(message["command"], (node, message["request_id"]))
            else:
                self.apply_command(message["command"], None)

        if message["type"] == "apply_command_response":
            request_id = message["request_id"]
            error = message.get("error", None)
            callback = self.__commands_waiting_reply.pop(request_id, None)
            if callback is not None:
                if error is not None:
                    callback(None, error)
                else:
                    idx = message["log_idx"]
                    term = message["log_term"]
                    assert idx > self.__raft_last_applied
                    self.__commands_waiting_commit[idx].append((term, callback))

        if self.__raft_state == RaftState.CANDIDATE:
            if (
                message["type"] == "response_vote"
                and message["term"] == self.__raft_current_term
            ):
                self.__votes_count += 1

                if self.__votes_count > (len(self.__other_nodes) + 1) / 2:
                    self.__on_become_leader()

        if self.__raft_state == RaftState.LEADER:
            if message["type"] == "next_node_idx":
                reset = message["reset"]
                next_node_idx = message["next_node_idx"]
                success = message["success"]

                current_node_idx = next_node_idx - 1
                if reset:
                    self.__raft_next_index[node] = next_node_idx
                if success:
                    if self.__raft_match_index[node] < current_node_idx:
                        self.__raft_match_index[node] = current_node_idx
                        self.__raft_next_index[node] = next_node_idx
                self.__last_response_time[node] = monotonic_time()

    def __call_err_callback(self, err, callback):
        if callback is None:
            return
        if isinstance(callback, tuple):
            request_node, request_id = callback
            self.__transport.send(
                request_node,
                {
                    "type": "apply_command_response",
                    "request_id": request_id,
                    "error": err,
                },
            )
            return
        callback(None, err)

    def __sendNextNodeIdx(self, node, reset=False, nextNodeIdx=None, success=False):
        if nextNodeIdx is None:
            nextNodeIdx = self.__get_current_log_index() + 1
        self.__transport.send(
            node,
            {
                "type": "next_node_idx",
                "next_node_idx": nextNodeIdx,
                "reset": reset,
                "success": success,
            },
        )

    def __generate_raft_timeout(self):
        min_timeout = self.__conf.raft_min_timeout
        max_timeout = self.__conf.raft_max_timeout
        return min_timeout + (max_timeout - min_timeout) * random.random()

    def __on_readonly_node_connected(self, node):
        self.__readonly_nodes.add(node)
        self.__connected_nodes.add(node)
        self.__raft_next_index[node] = self.__get_current_log_index() + 1
        self.__raft_match_index[node] = 0

    def __on_readonly_node_disconnected(self, node):
        self.__readonly_nodes.discard(node)
        self.__connected_nodes.discard(node)
        self.__raft_next_index.pop(node, None)
        self.__raft_match_index.pop(node, None)
        node.destroy()

    def __on_node_connected(self, node):
        self.__connected_nodes.add(node)

    def __on_node_disconnected(self, node):
        self.__connected_nodes.discard(node)

    def __get_current_log_index(self):
        return self.__raft_log[-1][1]

    def __get_current_log_term(self):
        return self.__raft_log[-1][2]

    def __get_prev_log_index_term(self, next_node_index):
        prev_index = next_node_index - 1
        entries = self.__get_entries(prev_index, 1)
        if entries:
            return prev_index, entries[0][2]
        return None, None

    def __get_entries(self, from_i_dx, count=None, max_size_bytes=None):
        first_entry_i_dx = self.__raft_log[0][1]
        if from_i_dx is None or from_i_dx < first_entry_i_dx:
            return []
        diff = from_i_dx - first_entry_i_dx
        if count is None:
            result = self.__raft_log[diff:]
        else:
            result = self.__raft_log[diff: diff + count]
        if max_size_bytes is None:
            return result
        total_size = 0
        i = 0
        for i, entry in enumerate(result):
            total_size += len(entry[0])
            if total_size >= max_size_bytes:
                break
        return result[: i + 1]

    def _is_leader(self):
        """Check if current node has a leader state.
        WARNING: there could be multiple leaders at the same time!

        :return: True if leader, False otherwise
        :rtype: bool
        """
        return self.__raft_state == RaftState.LEADER

    def get_leader(self):
        """Returns last known leader.

        WARNING: this information could be outdated, e.g. there could be another leader selected!
        WARNING: there could be multiple leaders at the same time!

        :return: the last known leader node.
        :rtype: Node
        """
        return self.__raft_leader

    def is_ready(self):
        """Check if current node is initially synced with others and has an actual data.

        :return: True if ready, False otherwise
        :rtype: bool
        """
        return self.__on_ready_called

    def _is_ready(self):
        return self.is_ready()

    def _get_term(self):
        return self.__raft_current_term

    def _get_raft_log_size(self):
        return len(self.__raft_log)

    def __delete_entries_from(self, from_i_dx):
        first_entry_i_dx = self.__raft_log[0][1]
        diff = from_i_dx - first_entry_i_dx
        if diff < 0:
            return
        self.__raft_log.deleteEntriesFrom(diff)

    def __delete_entries_to(self, toIDx):
        first_entry_i_dx = self.__raft_log[0][1]
        diff = toIDx - first_entry_i_dx
        if diff < 0:
            return
        self.__raft_log.deleteEntriesTo(diff)

    def __on_become_leader(self):
        self.__raft_leader = self.__self_node
        self.__set_state(RaftState.LEADER)

        self.__last_response_time.clear()
        for node in self.__other_nodes | self.__readonly_nodes:
            self.__raft_next_index[node] = self.__get_current_log_index() + 1
            self.__raft_match_index[node] = 0
            self.__last_response_time[node] = monotonic_time()

        # No-op command after leader election.
        idx, term = self.__get_current_log_index() + 1, self.__raft_current_term
        self.__raft_log.add(_wchar(CommandType.NO_OP.value), idx, term)
        self.__noop_i_dx = idx
        if not self.__conf.append_entries_use_batch:
            self.__send_append_entries()

        self.__send_append_entries()

    def __set_state(self, new_state: RaftState):
        old_state = self.__raft_state
        self.__raft_state = new_state
        callback = self.__conf.on_state_changed
        if callback is not None and old_state != new_state:
            callback(old_state, new_state)

    def __on_leader_changed(self):
        for id in sorted(self.__commands_waiting_reply):
            self.__commands_waiting_reply[id](None, FailReason.LEADER_CHANGED)
        self.__commands_waiting_reply = {}

    def __send_append_entries(self):
        self.__new_append_entries_time = monotonic_time() + self.__conf.append_entries_period

        start_time = monotonic_time()

        batch_size_bytes = self.__conf.append_entries_batch_size_bytes

        for node in self.__other_nodes | self.__readonly_nodes:
            if node not in self.__connected_nodes:
                self.__serializer.cancelTransmisstion(node)
                continue

            send_single = True
            sending_serialized = False
            next_node_index = self.__raft_next_index[node]

            while (
                next_node_index <= self.__get_current_log_index()
                or send_single
                or sending_serialized
            ):
                if next_node_index > self.__raft_log[0][1]:
                    prev_log_idx, prev_log_term = self.__get_prev_log_index_term(next_node_index)
                    entries = []
                    if next_node_index <= self.__get_current_log_index():
                        entries = self.__get_entries(next_node_index, None, batch_size_bytes)
                        self.__raft_next_index[node] = entries[-1][1] + 1

                    if len(entries) == 1 and len(entries[0][0]) >= batch_size_bytes:
                        entry = pickle.dumps(entries[0])
                        for pos in range(0, len(entry), batch_size_bytes):
                            curr_data = entry[pos : pos + batch_size_bytes]
                            if pos == 0:
                                transmission = "start"
                            elif pos + batch_size_bytes >= len(entries[0][0]):
                                transmission = "finish"
                            else:
                                transmission = "process"
                            message = {
                                "type": "append_entries",
                                "transmission": transmission,
                                "data": curr_data,
                                "term": self.__raft_current_term,
                                "commit_index": self.__raft_commit_index,
                                "prev_log_idx": prev_log_idx,
                                "prev_log_term": prev_log_term,
                            }
                            self.__transport.send(node, message)
                            if node not in self.__connected_nodes:
                                break
                    else:
                        message = {
                            "type": "append_entries",
                            "term": self.__raft_current_term,
                            "commit_index": self.__raft_commit_index,
                            "entries": entries,
                            "prev_log_idx": prev_log_idx,
                            "prev_log_term": prev_log_term,
                        }
                        self.__transport.send(node, message)
                        if node not in self.__connected_nodes:
                            break
                else:
                    transmission_data = self.__serializer.getTransmissionData(node)
                    message = {
                        "type": "append_entries",
                        "term": self.__raft_current_term,
                        "commit_index": self.__raft_commit_index,
                        "serialized": transmission_data,
                    }
                    self.__transport.send(node, message)
                    if node not in self.__connected_nodes:
                        break

                    if transmission_data is not None:
                        isLast = transmission_data[2]
                        if isLast:
                            self.__raft_next_index[node] = self.__raft_log[1][1] + 1
                            sending_serialized = False
                        else:
                            sending_serialized = True
                    else:
                        sending_serialized = False

                next_node_index = self.__raft_next_index[node]

                send_single = False

                delta = monotonic_time() - start_time
                if delta > self.__conf.append_entries_period:
                    break

    def __connected_to_anyone(self):
        return len(self.__connected_nodes) > 0 or len(self.__other_nodes) == 0

    def _get_conf(self):
        return self.__conf


    def _get_encryptor(self):
        return self.__encryptor


    def __change_cluster(self, request):
        if self.__raft_last_applied < self.__noop_i_dx:
            # No-op entry was not commited yet
            return False

        if self.__change_cluster_i_dx is not None:
            if self.__raft_last_applied >= self.__change_cluster_i_dx:
                self.__change_cluster_i_dx = None

        # Previous cluster change request was not commited yet
        if self.__change_cluster_i_dx is not None:
            return False

        return self.__do_change_cluster(request)


    def __do_change_cluster(self, request, reverse=False):
        request_type = request[0]
        request_node_id = request[1]
        if len(request) >= 3:
            request_node = request[2]
            if not isinstance(
                request_node, TCPNode
            ):  # Actually shouldn't be necessary, but better safe than sorry.
                request_node = self.__node_class(request_node)
        else:
            request_node = self.__node_class(request_node_id)

        if request_type == "add":
            adding = not reverse
        elif request_type == "rem":
            adding = reverse
        else:
            return False

        if adding:
            new_node = request_node
            # Node already exists in cluster
            if new_node == self.__self_node or new_node in self.__other_nodes:
                return False
            self.__other_nodes.add(new_node)
            self.__raft_next_index[new_node] = self.__get_current_log_index() + 1
            self.__raft_match_index[new_node] = 0
            if self._is_leader():
                self.__last_response_time[new_node] = monotonic_time()
            self.__transport.add_node(new_node)
            return True
        else:
            oldNode = request_node
            if oldNode == self.__self_node:
                return False
            if oldNode not in self.__other_nodes:
                return False
            self.__other_nodes.discard(oldNode)
            self.__raft_next_index.pop(oldNode, None)
            self.__raft_match_index.pop(oldNode, None)
            self.__transport.drop_node(oldNode)
            return True

    def __try_log_compaction(self):
        curr_time = monotonic_time()
        serialize_state, serialize_id = self.__serializer.checkSerializing()

        if serialize_state == SerializerState.SUCCESS:
            self.__last_serialized_time = curr_time
            self.__delete_entries_to(serialize_id)
            self.__last_serialized_entry = serialize_id

        if serialize_state == SerializerState.FAILED:
            logger.warning("Failed to store full dump")

        if serialize_state != SerializerState.NOT_SERIALIZING:
            return

        if (
            len(self.__raft_log) <= self.__conf.log_compaction_min_entries
            and curr_time - self.__last_serialized_time <= self.__conf.log_compaction_min_time
            and not self.__force_log_compaction
        ):
            return


        self_node = self.transform_node({self.__self_node}).pop()
        other_nodes = self.transform_node(self.__other_nodes)
        if self.__conf.log_compaction_split:
            all_node_ids = sorted(
                [node.id for node in (other_nodes | {self_node})]
            )
            nodes_count = len(all_node_ids)
            self_idx = all_node_ids.index(self_node.id)
            interval = self.__conf.log_compaction_min_time
            period_start = int(curr_time / interval) * interval
            node_interval = float(interval) / nodes_count
            node_interval_start = period_start + self_idx * node_interval
            node_interval_end = node_interval_start + 0.3 * node_interval
            if curr_time < node_interval_start or curr_time >= node_interval_end:
                return

        self.__force_log_compaction = False

        last_applied_entries = self.__get_entries(self.__raft_last_applied - 1, 2)
        if (
            len(last_applied_entries) < 2
            or last_applied_entries[0][1] == self.__last_serialized_entry
        ):
            self.__last_serialized_time = curr_time
            return

        if self.__conf.serializer is None:
            self_data = dict(
                [(k, v) for k, v in self.__dict__.items() if k not in self.__properties]
            )
            data = self_data
            if self.__consumers:
                data = [self_data]
                for consumer in self.__consumers:
                    data.append(consumer._serialize())
        else:
            data = None
        cluster = self.__other_nodes | {self.__self_node}
        self.__serializer.serialize(
            (data, last_applied_entries[1], last_applied_entries[0], cluster),
            last_applied_entries[0][1],
        )

    def __load_dump_file(self, clear_journal):
        try:
            data = self.__serializer.deserialize()
            if data[0] is not None:
                if self.__consumers:
                    self_data: dict = data[0][0]
                    consumers_data = data[0][1:]
                else:
                    self_data: dict = data[0]
                    consumers_data = []

                for k, v in self_data.items():
                    self.__dict__[k] = v

                for i, consumer in enumerate(self.__consumers):
                    consumer._deserialize(consumers_data[i])

            if (
                clear_journal
                or len(self.__raft_log) < 2
                or self.__raft_log[0] != data[2]
                or self.__raft_log[1] != data[1]
            ):
                self.__raft_log.clear()
                self.__raft_log.add(*data[2])
                self.__raft_log.add(*data[1])

            self.__raft_last_applied = data[1][1]

            if self.__conf.dynamic_membership_change:
                self.__update_cluster_configuration(
                    {node for node in data[3] if node != self.__self_node}
                )
            self.__on_set_code_version(0)
        except:
            logger.exception("failed to load full dump")

    def transform_node(self, nodes: set[TCPNode | str]) -> set[TCPNode]:
        return {self.__node_class(node) if not isinstance(node, TCPNode) else node for node in nodes}

    def __update_cluster_configuration(self, new_nodes: set[TCPNode | str]):
        # newNodes: list of Node or node ID
        new_nodes_node: set[TCPNode] = self.transform_node(new_nodes)
        other_nodes_node: set[TCPNode] = self.transform_node(self.__other_nodes)
        nodes_to_remove: set[TCPNode] = other_nodes_node - new_nodes_node
        nodes_to_add = new_nodes - other_nodes_node
        for node in nodes_to_remove:
            self.__raft_next_index.pop(node, None)
            self.__raft_match_index.pop(node, None)
            self.__transport.drop_node(node)
        self.__other_nodes = new_nodes
        for node in nodes_to_add:
            self.__transport.add_node(node)
            self.__raft_next_index[node] = self.__get_current_log_index() + 1
            self.__raft_match_index[node] = 0

    def _configure_dns_resolver(self):
        # Configure DNS resolver
        global_dns_resolver().set_timeouts(
            self.__conf.dns_cache_time, self.__conf.dns_fail_cache_time
        )
        global_dns_resolver().set_preferred_addr_family(self.__conf.preferred_addr_type)
