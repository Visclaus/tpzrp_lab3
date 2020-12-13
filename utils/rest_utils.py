import struct
from enum import Enum
from urllib import parse, request


class MsgType(Enum):
    Bitfield = 5
    Have = 4
    Choke = 0
    Unchoke = 1
    Piece = 7


def get_message_length(peer_socket):
    data = peer_socket.recv(4)
    return struct.unpack('!I', data)[0]


def send_interested_msg(peer_socket):
    data = struct.pack('!I', 1) + struct.pack('!B', 2)
    peer_socket.sendall(data)


def send_not_interested_msg(peer_socket):
    data = struct.pack('!I', 1) + struct.pack('!B', 3)
    peer_socket.sendall(data)


def get_data(length, peer_socket):
    data = b''
    while length > 0:
        raw_data = peer_socket.recv(length)
        if raw_data == b'':
            break
        peer_socket.settimeout(5)
        data += raw_data
        length -= len(raw_data)
    return data


def make_get_request(url: str, params: dict):
    full_url = f"{url}?{parse.urlencode(params)}"
    req = request.Request(full_url, method="GET")
    try:
        response = request.urlopen(req)
    except Exception as ex:
        print("GET request error")
        raise ex
    return response
