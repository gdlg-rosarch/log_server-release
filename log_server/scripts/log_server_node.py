#!/usr/bin/env python

import rospy
from rosgraph_msgs.msg import Log
from expiringdict import ExpiringDict
import shelve
import tempfile
import os
from log_server.srv import BoundedLogBatch, BoundedLogBatchResponse
from log_server.srv import LogBatch, LogStreamResponse, LogBatchResponse
from log_server.srv import LogStream as LogStreamSrv
from log_server.msg import Log as ServerLog
import time
import hashlib
import bisect

class Cache(object):

    def __init__(self, store_file):
        self.cache = ExpiringDict(max_len=1000, max_age_seconds=1000)
        self.store = shelve.open(store_file)

    def get(self, key):
        if key in self.cache:
            return self.cache.get(key)
        else:
            return self.store[str(key)]

    def add(self, key, value):
        self.cache[key] = value
        self.store[str(key)] = value

class LogCache(Cache):

    def __init__(self, store_file):
        super(LogCache, self).__init__(store_file)
        self.paths_ordered_logs = {}

    def filter(self, log_filter):

        log_ids = []

        for path in self.paths_ordered_logs:
            if log_filter.match_paths(path):
                levels = self.paths_ordered_logs[path]
                for level in levels:
                    if log_filter.match_level(level):
                        log_ids.extend(levels[level])

        return log_ids

    def add(self, key, log):
        if log.name not in self.paths_ordered_logs:
            self.paths_ordered_logs[log.name] = {
                Log.DEBUG: [],
                Log.INFO: [],
                Log.WARN: [],
                Log.ERROR: [],
                Log.FATAL: []}
        self.paths_ordered_logs[log.name][log.level].append(key)
        super(LogCache, self).add(key, log)

class LogFilter(object):

    def __init__(self, paths, level):
        self.paths = paths
        self.level = level

    def match(self, log):
        return (self.match_paths(log.name) and self.match_level(log.level))

    def match_paths(self, name):
        return any([name.startswith(path) for path in self.paths])

    def match_level(self, level):
        return level >= self.level

    def __hash__(self):
        return abs(hash((tuple(list(set(self.paths))), self.level)))


class Watchdog(object):

    def __init__(self, timeout=60):
        self.timeout = timeout
        self.kick()

    def kick(self):
        self.start_time = time.time()

    def is_expired(self):
        now = time.time()
        elapsed_time = now - self.start_time
        return (elapsed_time > self.timeout)


class LogStream(Watchdog):

    def __init__(self, topic_name, timeout=60):
        super(self.__class__, self).__init__(timeout)
        self.topic = rospy.Publisher(topic_name, ServerLog, queue_size=0)

    def publish(self, log):
        msg = ServerLog(log)
        self.topic.publish(msg)


class LogStreams(object):

    def __init__(self):
        self.log_streams = {}

    def get_topic_name(self, paths, level):

        log_filter = LogFilter(paths, level)
        topic_name = '~{}'.format(hash(log_filter))

        if log_filter in self.log_streams:
            self.log_streams[log_filter].kick()
        else:
            self.log_streams[log_filter] = LogStream(topic_name)

        absolute_topic_name = rospy.resolve_name(topic_name)
        return absolute_topic_name

    def publish(self, log):
        for log_filter, log_stream in self.log_streams.iteritems():
            if log_filter.match(log):
                log_stream.publish(log)

    def purge(self):
        for key in self.log_streams.keys():
            log_stream = self.log_streams[key]
            if log_stream.is_expired():
                del self.log_streams[key]

class LogServer(object):

    def __init__(self):

        self.log_streams = LogStreams()
        self.last_log = None
        tmp = tempfile.mkdtemp()
        store_file = os.path.join(tmp, 'log_server')
        self.cache = LogCache(store_file)

    def on_new_log(self, log):

        self.cache.add(log.header.seq, log)
        self.last_log = log.header.seq
        self.log_streams.publish(log)

    def log_stream(self, request):
        topic_name = self.log_streams.get_topic_name(request.paths, request.level)
        return LogStreamResponse(topic_name)

    def bounded_log_batch(self, request):

        log_filter = LogFilter(request.paths, request.level)
        first_log_id = self.get_log_id(request.first_id)
        last_log_id = self.get_log_id(request.last_id)
        logs = []

        if first_log_id <= last_log_id:
            increment = 1
        else:
            increment = -1

        for log in self.get_next_log(first_log_id, log_filter, last_log_id, increment):
            logs.append(log)

        return BoundedLogBatchResponse(logs)

    def log_batch(self, request):
        return self.handle_log_batch(request, 1)

    def reverse_log_batch(self, request):
        return self.handle_log_batch(request, -1)

    def handle_log_batch(self, request, increment=1):

        logs = []
        log_filter = LogFilter(request.paths, request.level)
        first_log_id = self.get_log_id(request.id)

        for log in self.get_next_log(first_log_id, log_filter, increment=increment):
            if len(logs) < request.count:
                logs.append(log)
            else:
                break

        return LogBatchResponse(logs)

    def get_log_id(self, raw_id):

        if raw_id >= 0:
            log_id = raw_id
        else:
            log_id = self.last_log + raw_id + 1

        if log_id > self.last_log:
            log_id = self.last_log
        elif log_id < 0:
            log_id = 0

        return log_id

    def get_next_log(self, first_log_id, log_filter, last_log_id=None, increment=1):

        log_ids = sorted(self.cache.filter(log_filter))

        if increment >= 0:
            first_log_id_index = bisect.bisect_left(log_ids, first_log_id)
            if last_log_id is not None:
                last_log_id_index = bisect.bisect_right(log_ids, last_log_id)
            else:
                last_log_id_index = len(log_ids)
            index_range = range(first_log_id_index, last_log_id_index)
            print(index_range)
            print([log_ids[i] for i in index_range])
        else:
            first_log_id_index = bisect.bisect_right(log_ids, first_log_id)
            if last_log_id is not None:
                last_log_id_index = bisect.bisect_left(log_ids, last_log_id)
            else:
                last_log_id_index = 0
            index_range = range(last_log_id_index, first_log_id_index)
            index_range.reverse()
            print(first_log_id_index, last_log_id_index)
            print(index_range)
            print([log_ids[i] for i in index_range])

        for log_id_index in index_range:
            log = self.get_log(log_ids[log_id_index])
            if log is not None:
                yield log

    def get_log(self, log_id, log_filter=None):

        try:
            log = self.cache.get(log_id)
        except KeyError:
            pass
        else:
            if log_filter is  None or log_filter.match(log):
                return log

        return None

    def purge_streams(self):
        self.log_streams.purge()


if __name__ == '__main__':

    log_server = LogServer()

    rospy.init_node('log_server')
    rospy.Subscriber("/rosout_agg", Log, log_server.on_new_log)
    rospy.Service('~log_stream', LogStreamSrv, log_server.log_stream)
    rospy.Service('~bounded_log_batch', BoundedLogBatch, log_server.bounded_log_batch)
    rospy.Service('~log_batch', LogBatch, log_server.log_batch)
    rospy.Service('~reverse_log_batch', LogBatch, log_server.reverse_log_batch)

    rate = rospy.Rate(30)

    while not rospy.is_shutdown():
        log_server.purge_streams()
