import pytest
import logging
from time import sleep
from etcd_utils import EtcdEndpoint
import docker
import re
import os


log = logging.getLogger(__name__)

DOCKER_PUBLISH_HOST = os.environ.get('DOCKER_PUBLISH_HOST',
                                     '127.0.0.1')

ETCD_IMAGE = 'quay.io/coreos/etcd:v3.3.11'


class EtcdTestCluster:
    def __init__(self, ident, size):
        self.containers = []
        self.network = None
        self.ident = ident
        self.size = size
        self.client = docker.from_env()

    def is_container_ready(self, container):
        try:
            container.exec_run('etcdctl member list')
            return True
        except Exception:
            return False

    def wait_ready(self, container):
        while not self.is_container_ready(container):
            sleep(1)

    def get_endpoints(self):
        for c in self.containers:
            c.reload()
        return [
            EtcdEndpoint(DOCKER_PUBLISH_HOST,
                         c.attrs['NetworkSettings']['Ports'][
                                 '2379/tcp'][0]['HostPort'])
            for c in self.containers]

    def down(self):
        for c in self.containers:
            c.remove(force=True)
        if self.network:
            self.network.remove()

    def rolling_restart(self):
        for c in self.containers:
            log.info('killing container %s' % c.name)
            c.kill()
            log.info('waiting for container %s to be ready' % c.name)
            self.wait_ready(c)

    def up(self):
        self.network = self.client.networks.create(name="etcd-%s" % self.ident)
        image_found = False
        for image in self.client.images.list():
            if ETCD_IMAGE in image.tags:
                image_found = True
        if not image_found:
            log.info('pulling image %s' % ETCD_IMAGE)
            self.client.images.pull(ETCD_IMAGE)
        initial_cluster = ','.join(
            ["etcd{x}-{n}=http://etcd{x}-{n}:2380".format(n=self.ident, x=x)
             for x in range(self.size)])
        self.containers = [
            self.client.containers.create(
                name="etcd%s-%s" % (i, self.ident),
                image='quay.io/coreos/etcd:v3.3.11',
                environment={
                    'ETCDCTL_API': '3',
                },
                command=['etcd', '--name=etcd%s-%s' % (i, self.ident),
                         '--advertise-client-urls=http://0.0.0.0:2379',
                         '--listen-client-urls=http://0.0.0.0:2379',
                         '--listen-peer-urls=http://0.0.0.0:2380',
                         '--initial-cluster', initial_cluster,
                         ],
                ports={'2379/tcp': None},
                network=self.network.name,
            )
            for i in range(self.size)]
        for c in self.containers:
            log.info('starting container %s' % c.name)
            c.start()
        for c in self.containers:
            log.info('wait for container %s to be ready' % c.name)
            self.wait_ready(c)
            c.reload()


@pytest.fixture
def etcd_cluster(request):
    node_name = re.sub(r"[^a-zA-Z0-9]+", "", request.node.name)
    cluster = EtcdTestCluster(ident=node_name, size=3)

    def fin():
        cluster.down()
    request.addfinalizer(fin)
    cluster.up()

    return cluster
