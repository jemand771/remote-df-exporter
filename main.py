import os

import paramiko
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client.registry import Collector
from prometheus_client import start_http_server


class DFCollector(Collector):
    def __init__(self, *args, **kwargs):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.connect_ssh()
        super().__init__(*args, **kwargs)


    def collect(self):
        label_names = ["hostname", "port", "username", "device", "fstype", "mountpoint"]
        gauge_size = GaugeMetricFamily(
            "remote_filesystem_size_bytes",
            "Size of remote filesystem in bytes as reported by df",
            labels=label_names,
        )
        gauge_used = GaugeMetricFamily(
            "remote_filesystem_used_bytes",
            "Used bytes on a remote filesystem as reported by df",
            labels=label_names,
        )
        gauge_available = GaugeMetricFamily(
            "remote_filesystem_available_bytes",
            "Available bytes on a remote filesystem as reported by df",
            labels=label_names,
        )
        if not self._ssh_alive:
            self.connect_ssh()
        stdin, stdout, stderr = self.ssh_client.exec_command("df -T -B1")
        for line in stdout.readlines()[1:]:
            device, fstype, size, used, available, percent, mountpoint = line.strip().split()
            label_values = [self.hostname, str(self.port), self.username, device, fstype, mountpoint]
            gauge_size.add_metric(label_values, int(size))
            gauge_used.add_metric(label_values, int(used))
            gauge_available.add_metric(label_values, int(available))
        yield gauge_size
        yield gauge_used
        yield gauge_available
    
    def connect_ssh(self):
        self.hostname = os.environ["HOSTNAME"]
        self.port = int(os.environ["PORT"])
        self.username = os.environ["USERNAME"]
        password = os.environ.get("PASSWORD")
        key_path = os.environ.get("KEY_PATH")
        if (password is None) + (key_path is None) != 1:
            raise ValueError("need to specify either PASSWORD or KEY_PATH")
        kwargs = dict(password=password) if password else dict(key_filename=key_path)
        self.ssh_client.connect(
            hostname=self.hostname,
            port=self.port,
            username=self.username,
            **kwargs,
        )

    @property
    def _ssh_alive(self):
        if not (transport := self.ssh_client.get_transport()):
            return False
        if not transport.is_active():
            return False
        try:
            transport.send_ignore()
        except EOFError:
            return False
        return True


REGISTRY.register(DFCollector())


if __name__ == "__main__":
    start_http_server(9100)[1].join()
