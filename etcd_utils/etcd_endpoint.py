from dataclasses import dataclass


@dataclass
class EtcdEndpoint:
    address: str
    port: int = 2379
    timeout: int = 2

    @staticmethod
    def from_s(s: str):
        parts = s.split(':')
        if len(parts) > 1:
            return EtcdEndpoint(parts[0])
        else:
            return EtcdEndpoint(parts[0], int(parts[1]))
