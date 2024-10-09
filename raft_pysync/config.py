class FailReason:
    SUCCESS = 0  #: Command successfully applied.
    QUEUE_FULL = 1  #: Commands queue full
    MISSING_LEADER = 2  #: Leader is currently missing (leader election in progress, or no connection)
    DISCARDED = 3  #: Command discarded (cause of new leader elected and another command was applied instead)
    NOT_LEADER = (
        4  #: Leader has changed, old leader did not have time to commit command.
    )
    LEADER_CHANGED = (
        5  #: Similar to NOT_LEADER - leader has changed without command commit.
    )
    REQUEST_DENIED = 6  #: Command denied


class SerializerState:
    NOT_SERIALIZING = 0  #: Serialization not started or already finished.
    SERIALIZING = 1  #: Serialization in progress.
    SUCCESS = 2  #: Serialization successfully finished (should be returned only one time after finished).
    FAILED = (
        3  #: Serialization failed (should be returned only one time after finished).
    )


class RaftPysyncObjectConf(object):
    """RaftPySync configuration object"""

    def __init__(self, **kwargs):

        #  Encrypt session with specified password.
        #  Install `cryptography` module to be able to set password.
        self.password = kwargs.get("password", None)

        # Disable autoTick if you want to call onTick manually.
        # Otherwise, it will be called automatically from separate thread.
        self.auto_tick = kwargs.get("auto_tick", True)
        self.auto_tick_period = kwargs.get("auto_tick_period", 0.05)

        # Commands queue is used to store commands before real processing.
        self.commands_queue_size = kwargs.get("commands_queue_size", 100000)

        # After randomly selected timeout (in range from minTimeout to maxTimeout)
        # leader considered dead, and leader election starts.
        self.raft_min_timeout = kwargs.get("raft_min_timeout", 0.4)

        # Same as raftMinTimeout
        self.raft_max_timeout = kwargs.get("raft_max_timeout", 1.4)

        # Interval of sending append_entries (ping) command.
        # Should be less than raftMinTimeout.
        self.append_entries_period = kwargs.get("append_entries_period", 0.1)

        # When no data received for connectionTimeout - connection considered dead.
        # Should be more than raftMaxTimeout.
        self.connection_timeout = kwargs.get("connection_timeout", 3.5)

        # Interval between connection attempts.
        # Will try to connect to offline nodes each connectionRetryTime.
        self.connection_retry_time = kwargs.get("connection_retry_time", 5.0)

        # When leader has no response from the majority of the cluster
        # for leaderFallbackTimeout - it will fall back to follower state.
        # Should be more than appendEntriesPeriod.
        self.leader_fallback_timeout = kwargs.get("leader_fallback_timeout", 30.0)

        # Send multiple entries in a single command.
        # Enabled (default) - improve overall performance (requests per second)
        # Disabled - improve single request speed (don't wait till batch ready)
        self.append_entries_use_batch = kwargs.get("append_entries_use_batch", True)

        # Max number of bytes per single append_entries command.
        self.append_entries_batch_size_bytes = kwargs.get(
            "append_entries_batch_size_bytes", 2**16
        )

        # Bind address (address:port). Default - None.
        # If None - selfAddress is used as bindAddress.
        # Could be useful if selfAddress is not equal to bindAddress.
        # E.g. with routers, nat, port forwarding, etc.
        self.bind_address = kwargs.get("bind_address", None)

        # Preferred address type. Default - ipv4.
        # None - no preferences, select random available.
        # ipv4 - prefer ipv4 address type, if not available us ipv6.
        # ipv6 - prefer ipv6 address type, if not available us ipv4.
        self.preferred_addr_type = kwargs.get("preferred_addr_type", "ipv4")

        # Size of send buffer for sockets.
        self.send_buffer_size = kwargs.get("send_buffer_size", 2 ** 16)

        # Size of receive for sockets.
        self.recv_buffer_size = kwargs.get("recv_buffer_size", 2 ** 16)

        # Time to cache dns requests (improves performance,
        # no need to resolve address for each connection attempt).
        self.dns_cache_time = kwargs.get("dns_cache_time", 600.0)

        # Time to cache failed dns request.
        self.dns_fail_cache_time = kwargs.get("dns_fail_cache_time", 30.0)

        # Log will be compacted after it reach minEntries size or
        # minTime after previous compaction.
        self.log_compaction_min_entries = kwargs.get("log_compaction_min_entries", 5000)

        # Log will be compacted after it reach minEntries size or
        # minTime after previous compaction.
        self.log_compaction_min_time = kwargs.get("log_compaction_min_time", 300)

        # If true - each node will start log compaction in separate time window.
        # eg. node1 in 12.00-12.10, node2 in 12.10-12.20, node3 12.20 - 12.30,
        # then again node1 12.30-12.40, node2 12.40-12.50, etc.
        self.log_compaction_split = kwargs.get("log_compaction_split", False)

        # Max number of bytes per single append_entries command
        # while sending serialized object.
        self.log_compaction_batch_size = kwargs.get("log_compaction_batch_size", 2 ** 16)

        # If true - commands will be enqueued and executed after leader detected.
        # Otherwise - `FAIL_REASON.MISSING_LEADER <#raft_pysync.FAIL_REASON.MISSING_LEADER>`_ error will be emitted.
        # Leader is missing when establishing connection or when election in progress.
        self.commands_wait_leader = kwargs.get("commands_wait_leader", True)

        # File to store full serialized object. Save full dump on disc when doing log compaction.
        # None - to disable store.
        self.full_dump_file = kwargs.get("full_dump_file", None)

        # File to store operations journal. Save each record as soon as received.
        self.journal_file = kwargs.get("journal_file", None)

        # Will try to bind port every bindRetryTime seconds until success.
        self.bind_retry_time = kwargs.get("bind_retry_time", 1.0)

        # Max number of attempts to bind port (default 0, unlimited).
        self.max_bind_retries = kwargs.get("max_bind_retries", 0)

        # This callback will be called as soon as RaftPysyncObject sync all data from leader.
        self.on_ready = kwargs.get("on_ready", None)

        # This callback will be called for every change of RaftPysyncObject state.
        # Arguments: onStateChanged(oldState, newState).
        # WARNING: there could be multiple leaders at the same time!
        self.on_state_changed = kwargs.get("onStateChanged", None)

        # If enabled - cluster configuration could be changed dynamically.
        self.dynamic_membership_change = kwargs.get("dynamic_membership_change", False)

        # Sockets poller:
        #  * `auto` - auto select best available on current platform
        #  * `select` - use select poller
        #  * `poll` - use poll poller
        self.poller_type = kwargs.get("poller_type", "auto")

        # Use fork if available when serializing on disk.
        self.use_fork = kwargs.get("use_fork", True)

        # Custom serialize function, it will be called when logCompaction (fullDump) happens.
        # If specified - there should be a custom deserializer too.
        # Arguments: serializer(fileName, data)
        # data - some internal stuff that is *required* to be serialized with your object data.
        self.serializer = kwargs.get("serializer", None)

        # Check custom serialization state, for async serializer.
        # Should return one of `SERIALIZER_STATE <#raft_pysync.SERIALIZER_STATE>`_.
        self.serialize_checker = kwargs.get("serialize_checker", None)

        # Custom deserialize function, it will be called when restore from fullDump.
        # If specified - there should be a custom serializer too.
        # Should return data - internal stuff that was passed to serialize.
        self.deserializer = kwargs.get("deserializer", None)

        # This callback will be called when cluster is switched to new version.
        # onCodeVersionChanged(oldVer, newVer)
        self.on_code_version_changed = kwargs.get("on_code_version_changed", None)

        # TCP socket keepalive
        # (keepalive_time_seconds, probe_intervals_seconds, max_fails_count)
        # Set to None to disable
        self.tcp_keepalive = kwargs.get("tcp_keepalive", (16, 3, 5))

    def validate(self):
        assert self.auto_tick_period > 0
        assert self.commands_queue_size >= 0
        assert self.raft_min_timeout > self.append_entries_period * 3
        assert self.raft_max_timeout > self.raft_min_timeout
        assert self.append_entries_period > 0
        assert self.leader_fallback_timeout > self.append_entries_period
        assert self.connection_timeout >= self.raft_max_timeout
        assert self.connection_retry_time >= 0
        assert self.append_entries_batch_size_bytes > 0
        assert self.send_buffer_size > 0
        assert self.recv_buffer_size > 0
        assert self.dns_cache_time >= 0
        assert self.dns_fail_cache_time >= 0
        assert self.log_compaction_min_entries >= 2
        assert self.log_compaction_min_time > 0
        assert self.log_compaction_batch_size > 0
        assert self.bind_retry_time > 0
        assert (self.deserializer is None) == (self.serializer is None)
        if self.serializer is not None:
            assert self.full_dump_file is not None
        assert self.preferred_addr_type in ("ipv4", "ipv6", None)
        if self.tcp_keepalive is not None:
            assert isinstance(self.tcp_keepalive, tuple)
            assert len(self.tcp_keepalive) == 3
            for i in range(3):
                assert isinstance(self.tcp_keepalive[i], int)
                assert self.tcp_keepalive[i] > 0
