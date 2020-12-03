import ipaddress
import blxr_rlp as rlp
class Endpoint(object):
    def serialize(self, obj):
        if not isinstance(obj, tuple):
            raise ValueError("Tuple of values (ip, udp port, tcp port) is expected.")

        ip, udp_port, tcp_port = obj

        ip_address = ipaddress.ip_address(ip)

        return list((ip_address.packed,
                     rlp.sedes.big_endian_int.serialize(udp_port),
                     rlp.sedes.big_endian_int.serialize(tcp_port)))

    def deserialize(self, serialized_data):
        if not isinstance(serialized_data, list) or len(serialized_data) != 3:
            raise ValueError("List with 3 items is expected")

        address = ipaddress.ip_address(serialized_data[0])

        return address.compressed, \
               rlp.sedes.big_endian_int.deserialize(serialized_data[1]), \
               rlp.sedes.big_endian_int.deserialize(serialized_data[2])
