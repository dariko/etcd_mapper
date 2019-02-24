from typing import Union
from etcd_utils import EtcdMapper
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from time import sleep
import pytest
import logging

log = logging.getLogger(__name__)


@dataclass_json
@dataclass
class _TestType:
    attr1: Union[str, None] = None
    attr2: Union[str, None] = None


test_key_parsing_data = [
    ("/a/<attr1>/<attr2>",
        {"/a/cane/gatto": {'attr1': 'cane', 'attr2': 'gatto'},
         "/b/cane/gatto": None,
         "/b/cane/gatto/mela": None},
     ),
    ("/a/<attr1>/b/<attr2>",
        {"/a/cane/b/gatto": {'attr1': 'cane', 'attr2': 'gatto'},
         "/b/cane/b/gatto": None,
         "/b/cane/b/gatto/mela": None},
     ),
    ("/a/<attr1>/b/<attr2>/fixed",
        {"/a/cane/b/gatto/fixed": {'attr1': 'cane', 'attr2': 'gatto'},
         "/b/cane/b/gatto/fixed": None,
         "/b/cane/b/gatto/mela/fixed": None},
     ),
]


@pytest.fixture
def e_mapper(etcd_cluster):
    em = EtcdMapper(_TestType, etcd_cluster.get_endpoints(), "/prefix/<attr1>")
    yield em
    em.stop()


@pytest.mark.parametrize("pattern, key_values_maps", test_key_parsing_data)
def test_key_parsing(pattern, key_values_maps):
    m = EtcdMapper(_TestType, [], pattern)
    log.info('testing pattern %s' % pattern)
    for k, v in key_values_maps.items():
        log.info('testing key %s' % k)
        cache_idx, path_attributes = m.cache_idx_from_key(k)
        assert path_attributes == v
        if cache_idx:
            assert m.key_from_element_id(cache_idx) == k


@pytest.mark.timeout(20)
def test_set_refresh_get(e_mapper):
    data = _TestType('foo', 'bar')
    e_mapper.set('foo', data, skiplocal=True)
    e_mapper.refresh()
    assert "foo" in e_mapper.cache
    assert e_mapper.cache["foo"] == data


@pytest.mark.timeout(30)
def test_events(e_mapper, mocker):
    data1 = _TestType('foo', 'bar1')
    data2 = _TestType('foo', 'bar2')
    e_mapper.refresh_on_start = False
    e_mapper.on_put = mocker.Mock()
    e_mapper.on_del = mocker.Mock()
    e_mapper.on_end = mocker.Mock()
    e_mapper.revision = 0
    e_mapper.run_thread()
    sleep(1)
    log.info('create new element')
    e_mapper.set('foo', data1, skiplocal=True)
    sleep(1)
    log.info('edit element')
    assert e_mapper.on_put.called_with(data1)
    e_mapper.on_put.reset_mock()
    e_mapper.set('foo', data2, skiplocal=True)
    sleep(1)
    assert e_mapper.on_put.called_with(data2)
    assert not e_mapper.on_del.called
    log.info('delete element')
    e_mapper.delete(data2.attr1, skiplocal=True)
    sleep(1)
    assert e_mapper.on_del.called_with(data2)
    assert not e_mapper.on_end.called
    log.info('stop mapper')
    e_mapper.stop()
    e_mapper.thread.join()
    assert e_mapper.on_end.called_with()


@pytest.mark.timeout(120)
def test_auto_reconnect(e_mapper, mocker, etcd_cluster):
    data1 = _TestType('foo', 'bar1')
    data2 = _TestType('foo', 'bar2')
    e_mapper.refresh_on_start = False
    e_mapper.set('foo', data1, skiplocal=True)
    e_mapper.on_put = mocker.Mock()
    e_mapper.on_del = mocker.Mock()
    e_mapper.on_end = mocker.Mock()
    e_mapper.run_thread()
    sleep(1)
    e_mapper.set('foo', data2, skiplocal=True)
    sleep(1)
    assert e_mapper.on_put.called_with(data2)
    e_mapper.on_put.reset_mock()
    for c in etcd_cluster.containers:
        current_endpoint = e_mapper.rotating_client.current_endpoint()
        etcd_node = [
            x for x in etcd_cluster.containers
            if x.attrs['NetworkSettings']['Ports'][
                       '2379/tcp'][0]['HostPort'] == current_endpoint.port][0]
        etcd_node.kill()
        while e_mapper.live:
            log.info('wait for mapper to not be live')
            sleep(1)
        while not e_mapper.live:
            log.info('wait for mapper to be live')
            sleep(1)
        log.info('start etcd node %s' % etcd_node.name)
        etcd_node.restart()
        etcd_cluster.wait_ready(etcd_node)
        etcd_node.reload()
        e_mapper.rotating_client.endpoints = etcd_cluster.get_endpoints()
    log.info('update data on etcd')
    e_mapper.set('foo', data1, skiplocal=True)
    sleep(1)
    log.info('verify callback has been called for updated data')
    assert e_mapper.on_put.called_with(data1)
