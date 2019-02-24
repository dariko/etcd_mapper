import etcd3  # type: ignore
from typing import Union, List, Dict, Callable
from threading import Thread
from dataclasses_json import DataClassJsonMixin   # type: ignore
from dataclasses import dataclass
from threading import Lock, Event
from etcd_utils import StoppableThread
from etcd_utils import EtcdRotatingClient, EtcdEndpoint
from requests.exceptions import ConnectionError as req_connectionerror
from urllib3.exceptions import MaxRetryError as urllib_maxretryerror  # type: ignore  # noqa
import re
from queue import Queue, Empty
import logging

log = logging.getLogger(__name__)


@dataclass
class EMapperFilter:
    field: str
    value: str


class EtcdMapper():
    def __init__(self,
                 _type: DataClassJsonMixin,
                 endpoints: List[EtcdEndpoint],
                 pattern: str,
                 filters: List[EMapperFilter] = [],
                 on_put=None, on_del=None, on_end=None,
                 *args, **kwargs,
                 ):
        self._type = _type
        self.filters = filters
        self.handler_root: Dict = {}
        self.pattern = self._parse_pattern(pattern)

        self.cache: Dict[str, DataClassJsonMixin] = {}
        self.event_handling_queue: Queue = Queue()
        self.event_handling_lock = Lock()
        self.on_put: Union[Callable[[DataClassJsonMixin], None], None] = on_put
        self.on_del: Union[Callable[[DataClassJsonMixin], None], None] = on_del
        self.on_end: Union[Callable[[], None], None] = on_end
        self.revision: Union[int, None] = None
        self.refresh_on_start = True
        self.thread = None
        self.quit_event = Event()
        self.loop_quit_event = Event()
        self.rotating_client = EtcdRotatingClient(endpoints)
        self.live = False

        super().__init__()

    def filter_element(self, element):
        for f in self.filters:
            if getattr(element, f['name']) != f['value']:
                log.debug("element %s rejected by filter %s" %
                          (element, f))
                return False
        return True

    def _parse_pattern(self, pattern):
        self.prefix = "/"
        part_re = re.compile('^<(.*)>$')
        parts = pattern.strip("/").split("/")
        while not part_re.match(parts[0]):
            self.prefix += "%s/" % parts[0]
            parts.pop(0)
        node = self.handler_root
        for part in parts:
            m = part_re.match(part)
            if m:
                capture_name = m.group(1)
                name, node = node.setdefault("capture", (capture_name, {}))
                assert name == capture_name, (
                    "Conflicting capture name %s vs %s" % (name, capture_name)
                )
            else:
                node = node.setdefault(part, {})

    def __getitem__(self, key):
        return self.cache[key]

    def set(self, index, value, skiplocal=False):
        key = self.key_from_element_id(index)
        data = value.to_json()
        old_value = self.cache.get(index, None)
        log.debug('setting %s from %s to %s (%s)' %
                  (index, old_value, value, skiplocal))
        if not skiplocal:
            self.cache[index] = value
        try:
            self.rotating_client.client.put(key, data)
        except Exception:  # pragma: no cover
            log.warn('error while writing, rollback')
            if old_value:
                self.cache[index] = old_value
            raise

    def delete(self,  index, skiplocal=False):
        key = self.prefix + index
        old_value = self.cache[index]
        if not skiplocal:
            del self.cache[index]
        try:
            self.rotating_client.client.delete_range(key)
        except Exception:  # pragma: no cover
            if old_value:
                self.cache[index] = old_value
            raise

    def index_from_key(self, key):
        return key.replace(self.prefix, '').split('/')[0]

    def handle_events(self):
        while not self.loop_quit_event.is_set():
            try:
                event = self.event_handling_queue.get(timeout=1)
            except Empty:
                continue
            if event is not None:
                log.debug('handling event %s' % event)
                self.handle_event(event)

    def key_from_element_id(self, element_id):
        element_id_parts = element_id.split("/")
        key_parts = []
        _node = self.handler_root

        while _node != {}:
            if "capture" in _node:
                attribute_name, _node = _node["capture"]
                key_parts.append(element_id_parts.pop(0))
            else:
                key = list(_node.keys())[0]
                key_parts.append(key)
                _node = _node[key]
        return "%s%s" % (self.prefix, "/".join(key_parts))

    def cache_idx_from_key(self, key, return_attributes=True):
        if not key.startswith(self.prefix):
            return None, None
        key = key[len(self.prefix):]
        key_parts = key.strip('/').split('/')
        handler_node = self.handler_root
        path_attributes = {}
        cache_idx_parts = []
        while key_parts:
            next_part = key_parts.pop(0)
            if "capture" in handler_node:
                attribute_name, handler_node = handler_node["capture"]
                path_attributes[attribute_name] = next_part
                cache_idx_parts.append(next_part)
            elif next_part in handler_node:
                handler_node = handler_node[next_part]
            else:
                log.debug("No matching sub-handler for %s ,%s," %
                          (key, next_part))
        return "/".join(cache_idx_parts), path_attributes

    def handle_event(self, event=None, data=None, key=None, _type=None):
        refresh_event = True
        if event is not None:
            data = event.value
            key = event.key
            _type = event.type
            refresh_event = False

        key = key.decode('utf8')
        element_id, path_attributes = self.cache_idx_from_key(key)

        if _type == etcd3.models.EventEventType.PUT:
            log.debug("event PUT %s: %s" % (key, data))
            try:
                element = self._type.from_json(data)
                for attribute, value in path_attributes.items():
                    setattr(element, attribute, value)
            except Exception as ex:
                log.error("error parsing %s from string %s: %s" %
                          (self._type, data, ex))
                return
            if not self.filter_element(element):
                return
            prev = None
            if element_id in self.cache:
                prev = self.cache[element_id]
                if element == self.cache[element_id]:
                    log.debug('element %s unchanged (%s -> %s)' %
                              (element_id, prev, element))
                    return  # skips callback calling after the if
                else:
                    self.cache[element_id] = element
                    log.debug('updated %s from %s to  %s' %
                              (element_id, prev, element))
            else:
                self.cache[element_id] = element
                log.debug('added %s %s: %s' %
                          (self._type, element_id, element))
            if self.on_put is not None and (not refresh_event):
                self.on_put(element)
        else:  # if _type == etcd3.models.EventEventType.DELETE:
            log.debug("event DELETE %s" % key)
            if element_id in self.cache:
                element = self.cache[element_id]
                del self.cache[element_id]
                log.debug('removed idx: %s element: %s' %
                          (element_id, element))
                if self.on_del is not None and (not refresh_event):
                    self.on_del(element)
            else:
                log.warn("Can't delete missing element %s" % key)

    def refresh(self) -> int:
        data = self.rotating_client.client.range(self.prefix, prefix=True)
        self.cache = {}
        if data.kvs:
            for kv in data.kvs:
                log.debug('submitting fake PUT event from '
                          'refresh, kv: %s' % kv)
                self.handle_event(key=kv.key, data=kv.value,
                                  _type=etcd3.models.EventEventType.PUT)
        return data.header.revision

    def run_thread(self):
        assert not self.isAlive()
        self.thread = StoppableThread(target=self.run)
        self.thread.start()

    def isAlive(self):
        return self.thread is not None and self.thread.isAlive()

    def stop(self, wait=True):
        self.quit_event.set()
        self.loop_quit_event.set()
        if wait:
            if self.thread:
                self.thread.join()

    def run(self):
        firstloop = True
        self.quit_event.clear()
        while firstloop or not self.quit_event.wait(1):
            firstloop = False
            try:
                self.loop_quit_event.clear()
                self._run()
            except (req_connectionerror, urllib_maxretryerror) as e:
                log.error('connection error on emapper loop: %s' % e)
            except Exception:  # pragma: no cover
                log.exception('error in watch loop')
            finally:
                self.rotating_client.rotate()

    def _run(self):
        watcher = None
        watcher_thread = None
        event_handler_thread = None
        try:
            if self.refresh_on_start or (self.revision is None):
                self.revision = self.refresh() + 1
                log.info(("refreshed %s elements from %s at "
                         "revision %s") % (len(self.cache),
                         self.prefix, self.revision - 1))
            log.debug("starting watch on %s at revision %s" %
                      (self.prefix, self.revision))

            def watcher_event(event):
                self.event_handling_queue.put(event)

            watcher = self.rotating_client.client.Watcher(
                key=self.prefix, prefix=True, start_revision=self.revision,
                progress_notify=True, max_retries=0)
            watcher.onEvent(watcher_event)
            watcher_thread = Thread(target=watcher.run)
            watcher_thread.start()

            event_handler_thread = Thread(target=self.handle_events)
            event_handler_thread.start()
            while not self.loop_quit_event.wait(timeout=1):
                log.debug(self.rotating_client.client.status())
                self.live = True
                if not watcher.watching:
                    log.error('watcher not watching, '
                              'stopping mapper iteration')
                    break
        finally:
            self.live = False
            self.loop_quit_event.set()
            if watcher:
                watcher.stop()
            log.debug('wait for watcher thread')
            if watcher_thread and watcher_thread.isAlive():
                watcher_thread.join()
            log.debug('wait for event handler thread')
            if event_handler_thread:
                event_handler_thread.join()
            if self.on_end:
                log.debug('notifying on_end callback')
                self.on_end()
