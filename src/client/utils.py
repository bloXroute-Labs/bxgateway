#
# Copyright (C) 2016, Cornell University, All rights reserved.
# See the file COPYING for details.
#
# Utility classes
#
import os
import sys
import time
from collections import deque
from datetime import datetime
from heapq import *
from threading import Condition, Lock, Thread

from pympler import muppy, summary, tracker

import options

DUMP_LOG = False
MAX_ERR_QUEUE_SIZE = 30
error_msgs = deque()


##
# The Inputbuffer Interface
##

class InputBuffer(object):
    def __init__(self):
        self.input_list = deque()
        self.length = 0

    def endswith(self, suffix):
        if self.input_list:
            return False

        if len(self.input_list[-1]) >= len(suffix):
            return self.input_list[-1].endswith(suffix)
        else:
            # FIXME self_input_list, self.suffix are undefined
            raise RuntimeError("FIXME")

        # elif len(self_input_list) > 1:
        #     first_len = len(self.input_list[-1])
        #
        #     return self.suffix[-first_len:] == self.input_list[-1] and \
        #            self.input_list[-2].endswith(self.suffix[:-first_len])
        # return False

    # Adds a bytearray to the end of the input buffer.
    def add_bytes(self, piece):
        assert isinstance(piece, bytearray)
        self.input_list.append(piece)
        self.length += len(piece)

    # Removes the first num_bytes bytes in the input buffer and returns them.
    def remove_bytes(self, num_bytes):
        assert self.length >= num_bytes > 0

        to_return = bytearray(0)
        while self.input_list and num_bytes >= len(self.input_list[0]):
            next_piece = self.input_list.popleft()
            to_return.extend(next_piece)
            num_bytes -= len(next_piece)
            self.length -= len(next_piece)

        assert self.input_list or num_bytes == 0

        if self.input_list:
            to_return.extend(self.input_list[0][:num_bytes])
            self.input_list[0] = self.input_list[0][num_bytes:]
            self.length -= num_bytes

        return to_return

    # Returns the first bytes_to_peek bytes in the input buffer.
    # The assumption is that these bytes are all part of the same message.
    # Thus, we combine pieces if we cannot just return the first message.
    def peek_message(self, bytes_to_peek):
        if bytes_to_peek > self.length:
            return bytearray(0)

        while bytes_to_peek > len(self.input_list[0]):
            head = self.input_list.popleft()
            head.extend(self.input_list.popleft())
            self.input_list.appendleft(head)

        return self.input_list[0]

    # Gets a slice of the inputbuffer from start to end.
    # We assume that this slice is a piece of a single bitcoin message
    # for performance reasons (with respect to the number of copies).
    # Additionally, the start value of the slice must exist.
    def get_slice(self, start, end):
        assert self.length >= start

        # Combine all of the pieces in this slice into the first item on the list.
        # Since we will need to do so anyway when handing the message.
        while end > len(self.input_list[0]) and len(self.input_list) > 1:
            head = self.input_list.popleft()
            head.extend(self.input_list.popleft())
            self.input_list.appendleft(head)

        return self.input_list[0][start:end]


##
# The Outputbuffer Interface ##
##

# There are three key functions on the outputbuffer read interface. This should also
# be implemented by the cut through sink interface.
#   - has_more_bytes(): Whether or not there are more bytes in this buffer.
#   - get_buffer(): some bytes to send in the outputbuffer
#   - advance_buffer(): Advances the buffer by some number of bytes
class OutputBuffer(object):
    EMPTY = bytearray(0)  # The empty outputbuffer

    def __init__(self):
        # A deque of memoryview objects representing the raw memoryviews of the messages
        # that are being sent on the outputbuffer.
        self.output_msgs = deque()

        # Offset into the first message of the output_msgs
        self.index = 0

        # The total sum of all of the messages in the outputbuffer
        self.length = 0

    # Gets a non-empty memoryview buffer
    def get_buffer(self):
        if not self.output_msgs:
            raise RuntimeError("FIXME")
            # FIXME Output buffer is undefined
            # return OutputBufffer.EMPTY

        return self.output_msgs[0][self.index:]

    def advance_buffer(self, num_bytes):
        self.index += num_bytes
        self.length -= num_bytes

        assert self.index <= len(self.output_msgs[0])

        if self.index == len(self.output_msgs[0]):
            self.index = 0
            self.output_msgs.popleft()

    def at_msg_boundary(self):
        return self.index == 0

    def enqueue_msgbytes(self, msg_bytes):
        self.output_msgs.append(msg_bytes)
        self.length += len(msg_bytes)

    def prepend_msg(self, msg_bytes):
        if self.index == 0:
            self.output_msgs.appendleft(msg_bytes)
        else:
            prev_msg = self.output_msgs.popleft()
            self.output_msgs.appendleft(msg_bytes)
            self.output_msgs.appendleft(prev_msg)

        self.length += len(msg_bytes)

    def has_more_bytes(self):
        return self.length != 0


##
# The Alarm Interface
##

# Queue for events that take place at some time in the future.
class AlarmQueue(object):
    REMOVED = -1

    def __init__(self):
        # A list of alarm_ids, which contain three things:
        # [fire_time, unique_count, alarm]
        self.alarms = []
        self.uniq_count = 0  # Used for tiebreakers for heap comparison

        # dictionary from fn to a min-heap of scheduled alarms with that function.
        self.approx_alarms_scheduled = {}

    # fn(args) must return 0 if it was successful or a positive integer,
    # WAIT_TIME, to be rescheduled at a delay of WAIT_TIME in the future.
    def register_alarm(self, fire_delay, fn, *args):
        alarm = Alarm(fn, *args)
        alarm_id = [time.time() + fire_delay, self.uniq_count, alarm]
        heappush(self.alarms, alarm_id)
        self.uniq_count += 1
        return alarm_id

    # Register an alarm that will fire sometime between fire_delay +/- slop seconds from now.
    # If such an alarm exists (as told by the memory location of fn) already,
    def register_approx_alarm(self, fire_delay, slop, fn, *args):
        if fn not in self.approx_alarms_scheduled:
            new_alarm_id = self.register_alarm(fire_delay, fn, *args)
            self.approx_alarms_scheduled[fn] = []
            heappush(self.approx_alarms_scheduled[fn], new_alarm_id)
        else:
            now = time.time()
            late_time, early_time = fire_delay + now + slop, fire_delay + now - slop
            for alarm_id in self.approx_alarms_scheduled[fn]:
                if early_time <= alarm_id[0] <= late_time:
                    return

            heappush(self.approx_alarms_scheduled[fn], self.register_alarm(fire_delay, fn, *args))

    def unregister_alarm(self, alarm_id):
        alarm_id[-1] = AlarmQueue.REMOVED

        # Remove unnecessary alarms from the queue.
        while self.alarms[0][-1] == AlarmQueue.REMOVED:
            heappop(self.alarms)

    def fire_alarms(self):
        if not self.alarms:  # Nothing to do
            return

        curr_time = time.time()
        while self.alarms and self.alarms[0][0] <= curr_time:
            alarm_id = heappop(self.alarms)
            alarm = alarm_id[-1]

            if alarm != AlarmQueue.REMOVED:
                next_delay = alarm.fire()

                # Event wants to be rescheduled.
                if next_delay > 0:
                    alarm_id[0] = time.time() + next_delay
                    heappush(self.alarms, alarm_id)
                # Delete alarm from approx_alarms_scheduled (if applicable)
                elif alarm.fn in self.approx_alarms_scheduled:
                    alarm_heap = self.approx_alarms_scheduled[alarm.fn]

                    # Assert that the alarm that was just fired is the first one in the list
                    assert alarm_heap[0][1] == alarm_id[1]
                    heappop(alarm_heap)

                    if not alarm_heap:
                        del self.approx_alarms_scheduled[alarm.fn]

            elif alarm != AlarmQueue.REMOVED and alarm.fn in self.approx_alarms_scheduled:
                alarm_heap = self.approx_alarms_scheduled[alarm.fn]
                # Since the heap is a min-heap by alarm fire time, then the first
                # alarm should be the one that was just fired.
                assert alarm_heap[0][1] == alarm_id[1]

                heappop(alarm_heap)

    # Return tuple indicating <alarm queue empty, timeout>
    def time_to_next_alarm(self):
        if not self.alarms:
            return True, -1  # Nothing to do
        return False, self.alarms[0][0] - time.time()

    # Fires all alarms that have timed out on alarm_queue.
    # If the sender knows that some alarm will fire, then they can set has_alarm to be True.
    # Returns the timeout to the next alarm, -1 if there are no new alarms.
    def fire_ready_alarms(self, has_alarm):
        alarmq_empty, time_to_next_alarm = self.time_to_next_alarm()
        if has_alarm or (not alarmq_empty and time_to_next_alarm <= 0):
            # log_debug("AlarmQueue.fire_ready_alarms", "A timeout occurred")
            while not alarmq_empty and time_to_next_alarm <= 0:
                self.fire_alarms()
                alarmq_empty, time_to_next_alarm = self.time_to_next_alarm()

        return time_to_next_alarm


# An alarm object
class Alarm(object):
    def __init__(self, fn, *args):
        self.fn = fn
        self.args = args

    def fire(self):
        log_debug("Firing function {0} with args {1}".format(self.fn, self.args))
        return self.fn(*self.args)


##
# The Logging Interface
##

_hostname = '[Unassigned]'
_log_level = 0
_default_log = None
# The time (in seconds) to cycle through to another log.
LOG_ROTATION_INTERVAL = 24 * 3600


# Log class that you can write to which asynchronously dumps the log to the background
class Log(object):
    LOG_SIZE = 4096  # We flush every 4 KiB

    # No log should be bigger than 10 GB
    MAX_LOG_SIZE = 1024 * 1024 * 1024 * 10

    def __init__(self, path, use_stdout=False):
        self.log = []
        self.log_size = 0

        self.lock = Lock()
        self.needs_flush = Condition(self.lock)
        self.last_rotation_time = time.time()
        self.use_stdout = use_stdout
        if not self.use_stdout:
            if path is None or not path:
                path = "."
            self.filename = os.path.join(path,
                                         time.strftime("%Y-%m-%d-%H:%M:%S+0000-", time.gmtime()) + str(os.getpid()) +
                                         ".log")
        self.bytes_written = 0
        self.dumper = Thread(target=self.log_dumper)
        self.is_alive = True

        if not self.use_stdout:
            with open("current.log", "w") as log_file:
                log_file.write(self.filename)

        if options.ENABLE_LOGGING:
            self.dumper.start()

    def write(self, msg):
        if options.ENABLE_LOGGING:
            with self.lock:
                # sys.stdout.write(msg)
                self.log.append(msg)
                self.log_size += len(msg)

                if self.log_size >= Log.LOG_SIZE:
                    self.needs_flush.notify()

    def close(self):
        with self.lock:
            self.is_alive = False
            self.needs_flush.notify()

        self.dumper.join()

    def log_dumper(self):
        if self.use_stdout:
            output_dest = sys.stdout
        else:
            output_dest = open(self.filename, "a+")

        alive = True

        try:
            while alive:
                with self.lock:
                    alive = self.is_alive
                    while self.log_size < Log.LOG_SIZE and self.is_alive:
                        self.needs_flush.wait()

                    oldlog = self.log
                    oldsize = self.log_size
                    self.log = []
                    self.log_size = 0

                for msg in oldlog:
                    output_dest.write(msg)

                self.bytes_written += oldsize

                if options.FLUSH_LOG:
                    output_dest.flush()

                # Checks whether we've been dumping to this logfile for a while
                # and opens up a new file.
                now = time.time()
                if not self.use_stdout and now - self.last_rotation_time > LOG_ROTATION_INTERVAL \
                        or self.bytes_written > Log.MAX_LOG_SIZE:
                    self.last_rotation_time = now
                    self.filename = time.strftime("%Y-%m-%d-%H:%M:%S+0000-", time.gmtime()) + str(os.getpid()) + ".log"

                    output_dest.flush()
                    output_dest.close()

                    with open("current.log", "w") as current_log:
                        current_log.write(self.filename)

                    output_dest = open(self.filename, "a+")
                    self.bytes_written = 0

        finally:
            output_dest.flush()
            output_dest.close()


_log = None


# An enum that stores the different log levels
class LogLevel(object):
    def __init__(self):
        pass

    DEBUG = 0
    VERBOSE = 10
    WARNING = 20
    ERROR = 30
    CRASH = 40


# Logging helper functions

def log_init(path, use_stdout):
    global _log
    print "initializing log"
    _log = Log(path, use_stdout)


# Cleanly closes the log and flushes all contents to disk.
def log_close():
    _log.close()


def log_setmyname(name):
    global _hostname

    _hostname = '[' + name + ']'


def log(level, logtype, msg, log_time):
    global _hostname

    if level < _log_level:
        return  # No logging if it's not a high enough priority message.

    # loc is kept for debugging purposes. Uncomment the following line if you need to see the execution path.
    #    msg = loc + ": " + msg
    logmsg = "{0}: {1} [{2}]: {3}\n".format(
        _hostname, logtype, log_time.strftime("%Y-%m-%d-%H:%M:%S+%f"), msg)

    # Store all error messages to be sent to the frontend
    if level > LogLevel.WARNING:
        error_msgs.append(logmsg)

    if len(error_msgs) > MAX_ERR_QUEUE_SIZE:
        error_msgs.popleft()

    _log.write(logmsg)

    # Print out crash logs to the console for easy debugging.
    if DUMP_LOG or level == LogLevel.CRASH:
        sys.stdout.write(logmsg)


def log_debug(msg):
    log(LogLevel.DEBUG, "DEBUG  ", msg, datetime.utcnow())


def log_err(msg):
    log(LogLevel.ERROR, "ERROR  ", msg, datetime.utcnow())


def log_warning(msg):
    log(LogLevel.WARNING, "WARNING", msg, datetime.utcnow())


def log_crash(msg):
    log(LogLevel.CRASH, "CRASH  ", msg, datetime.utcnow())


def log_verbose(msg):
    log(LogLevel.VERBOSE, "VERBOSE", msg, datetime.utcnow())


##
# Transaction to ID Management
##

# A manager for the transaction mappings
# We assume in this class that no more than MAX_ID unassigned transactions exist at a time
class TransactionManager(object):
    # Size of a short id
    # If this is changed, make sure to change it in the TxAssignMessage
    SHORT_ID_SIZE = 4
    MAX_ID = 2 ** 32

    # The maximum amount of time that an ID is valid
    MAX_VALID_TIME = 600

    # Maximum amount of time we wait for the nodes to drop stale IDs
    MAX_SLOP_TIME = 120

    # is TransactionManager.MAX_VALID_TIME + TransactionManager.MAX_SLOP_TIME
    INITIAL_DELAY = 0

    NULL_TX = None

    def __init__(self, node):
        self.node = node

        # txhash is the longhash of the transaction, sid is the short ID for the transaction
        self.txhash_to_sid = {}
        # txid is the (unique) list of [time assigned, txhash]
        self.sid_to_txid = {}
        self.hash_to_contents = {}
        self.unassigned_hashes = set()
        self.tx_assignment_times = []
        self.tx_assign_alarm_scheduled = False

        self.relayed_txns = set()
        self.tx_relay_times = []

        self.prev_id = -1

        self.node.alarm_queue.register_alarm(TransactionManager.INITIAL_DELAY, self.assign_initial_ids)

    def assign_initial_ids(self):
        assert self.prev_id == -1

        if self.unassigned_hashes:
            # FIXME there is no method _assign_tx_to_sid
            raise RuntimeError("FIXME")

            # for tx_hash in self.unassigned_hashes:
            #     sid, tx_time = self.get_and_increment_id()
            #     self._assign_tx_to_sid(tx_hash, sid, tx_time)

        if self.tx_assignment_times:
            self.node.alarm_queue.register_alarm(TransactionManager.MAX_VALID_TIME, self.expire_old_ids)
            self.tx_assign_alarm_scheduled = True
        self.unassigned_hashes = None

    def get_and_increment_id(self):
        self.prev_id += 1
        while self.prev_id in self.sid_to_txid:
            self.prev_id += 1

            if self.prev_id > TransactionManager.MAX_ID:
                self.prev_id -= TransactionManager.MAX_ID

        return self.prev_id, time.time()

    def expire_old_ids(self):
        now = time.time()
        while self.tx_assignment_times and \
                now - self.tx_assignment_times[0][0] > TransactionManager.MAX_VALID_TIME:
            tx_id = heappop(self.tx_assignment_times)
            tx_hash = tx_id[1]

            if tx_hash != TransactionManager.NULL_TX:
                sid = self.txhash_to_sid[tx_hash]
                del self.txhash_to_sid[tx_hash]
                del self.sid_to_txid[sid]

        if self.tx_assignment_times:
            # Reschedule this function to be fired again after MAX_VALID_TIME seconds
            return TransactionManager.MAX_VALID_TIME
        else:
            self.tx_assign_alarm_scheduled = False
            return 0

    # Assigns the transaction to the given short id
    def assign_tx_to_sid(self, tx_hash, sid, tx_time):
        txid = [tx_time, tx_hash]

        self.txhash_to_sid[tx_hash] = sid
        self.sid_to_txid[sid] = txid
        heappush(self.tx_assignment_times, txid)

        if not self.tx_assign_alarm_scheduled:
            self.node.alarm_queue.register_alarm(TransactionManager.MAX_VALID_TIME, self.expire_old_ids)
            self.tx_assign_alarm_scheduled = True

    def assign_tx_to_id(self, tx_hash):
        assert self.node.is_manager

        # Not done waiting for the initial transactions to come through
        if self.unassigned_hashes is not None:
            if tx_hash not in self.unassigned_hashes:
                self.unassigned_hashes.add(tx_hash)
            return -1
        elif tx_hash not in self.txhash_to_sid:
            log_debug("XXX: Adding {0} to tx_hash mapping".format(tx_hash))
            sid, tx_time = self.get_and_increment_id()
            self.assign_tx_to_sid(tx_hash, sid, tx_time)
            return sid

    def get_txid(self, tx_hash):
        if tx_hash in self.txhash_to_sid:
            log_debug("XXX: Found the tx_hash in my mappings!")
            return self.txhash_to_sid[tx_hash]

        return -1

    def get_tx_from_sid(self, sid):
        if sid in self.sid_to_txid:
            tx_hash = self.sid_to_txid[sid][1]

            if tx_hash in self.hash_to_contents:
                return self.hash_to_contents[tx_hash]
            log_debug("Looking for hash: " + repr(tx_hash))
            log_debug("Could not find hash: " + repr(self.hash_to_contents.keys()[0:10]))

        return None

    # Returns True if
    def already_relayed(self, tx_hash):
        if tx_hash in self.relayed_txns:
            return True
        else:
            self.relayed_txns.add(tx_hash)
            heappush(self.tx_relay_times, [time.time(), tx_hash])
            self.node.alarm_queue.register_approx_alarm(2 * TransactionManager.MAX_VALID_TIME,
                                                        TransactionManager.MAX_VALID_TIME, self.cleanup_relayed)
            return False

    def cleanup_relayed(self):
        now = time.time()
        while self.tx_relay_times and \
                now - self.tx_relay_times[0][0] > TransactionManager.MAX_VALID_TIME:
            _, txhash = heappop(self.tx_relay_times)
            self.relayed_txns.remove(txhash)

        return 0


##
# The Memory Profiling Interface
##
class HeapProfiler(object):
    PROFILE_START = 0  # Time to start profiling
    PROFILE_INTERVAL = 300  # Profiling interval (in seconds)

    def __init__(self):
        print "options.PROFILING:"
        print options.PROFILING

        if not options.PROFILING:
            self.profiling = False
            return

        print "options.PROFILING:"
        print options.PROFILING
        self.profiling = True
        self.filename = ""
        self.last_rotation_time = time.time()

        tracker.SummaryTracker()

    def dump_profile(self):
        log_debug("Dumping heap profile!")

        # Assumption is that no one else will be printing while profiling is happening
        self.filename = "profiler-" + time.strftime("%Y-%m-%d-%H:%M:%S+0000", time.gmtime()) + ".prof"

        old_stdout = sys.stdout
        sys.stdout = open(self.filename, "a+")
        print "################# BEGIN NEW HEAP SNAPSHOT #################"
        all_objects = muppy.get_objects()
        print "Printing diff at time: " + time.strftime("%Y-%m-%d-%H:%M:%S+0000", time.gmtime())
        sum1 = summary.summarize(all_objects)
        summary.print_(sum1)
        print "Printing out all objects: "
        print "Index,Type, size"
        i = 0
        for obj in all_objects:
            print "{0},{1},{2}".format(i, type(obj), sys.getsizeof(obj))
            i += 1

        i = 0
        print "Index,Object"
        for obj in all_objects:
            print "{0},{1}".format(i, repr(obj))
            i += 1

        print "################## END NEW HEAP SNAPSHOT ##################"
        sys.stdout = old_stdout

        return self.PROFILE_INTERVAL
