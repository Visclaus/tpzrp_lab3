import hashlib
import socket
import struct
from random import choice
from string import digits

import bitstring


def calculate_hash(data):
    return hashlib.sha1(data).digest()


def calculate_peer_id():
    return ''.join(choice(digits) for _ in range(20))


def get_bitfield_data(raw_data):
    return map(lambda x: int(x), list(bitstring.BitArray(raw_data[1:]).bin))


def calculate_handshake(info_hash, peer_id):
    protocol_name = b'BitTorrent protocol'
    data = struct.pack('!B', len(protocol_name))
    data += protocol_name
    data += struct.pack('!Q', 0)
    data += info_hash
    data += bytes(peer_id, 'ascii')
    return data


def calculate_peers(peers):
    peers = [peers[i:i + 6] for i in range(0, len(peers), 6)]
    return [(socket.inet_ntoa(p[:4]), struct.unpack('!H', (p[4:]))[0]) for p in peers]
