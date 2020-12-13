import logging
from progress.bar import IncrementalBar
import threading
import time
import bencodepy
import validators
from constants import *
from utils.calulate_utils import *
from utils.file_utils import *
from utils.rest_utils import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('BitTorrent')


def set_have(raw_data, bitfield):
    piece_numb = struct.unpack('!I', raw_data[1:])[0]
    bitfield[piece_numb] = 1


def is_interested(bitfield, downloaded_pieces):
    for i in range(len(downloaded_pieces)):
        bit = bitfield[i] - downloaded_pieces[i]
        bitfield[i] = bit if bit > 0 else 0
    if sum(bitfield) > 0:
        return True
    else:
        return False


def send_block_request(peer_socket, index, length, begin=0):
    data = struct.pack('!BIII', 6, index, begin, length)
    data_s = struct.pack('!I', len(data)) + data
    peer_socket.sendall(data_s)


class BitTorrentClient(object):
    def __init__(self, path_to_torrent: str = "debian.torrent", path_to_save_dir: str = os.getcwd()):
        self.log_update_time = 6
        self.path_to_torrent = path_to_torrent
        self.torrent_dict = bencodepy.decode_from_file(self.path_to_torrent)
        self.info_hash = calculate_hash(bencodepy.encode(self.torrent_dict[b'info']))
        temp = self.torrent_dict[b'info'][b'pieces']
        self.pieces_hashes = [temp[i:i + 20] for i in range(0, len(temp), 20)]
        self.pieces_cnt = len(self.pieces_hashes)
        self.piece_length = self.torrent_dict[b'info'][b'piece length']
        self.peer_id = calculate_peer_id()
        self.file_name = self.torrent_dict[b'info'][b'name'].decode()
        self.full_result_filepath = path_to_save_dir + "/" + self.file_name
        self.received_pieces_mask = [0] * self.pieces_cnt
        self.full_length = self.calculate_full_length()
        self.downloaded_bytes = None
        self.connected_peers = []
        self.log_lock = threading.Lock()
        self.pieces_lock = threading.Lock()

    def calculate_full_length(self):
        length = 0
        if b'length' in self.torrent_dict[b'info'].keys():
            length = self.torrent_dict[b'info'][b'length']
        else:
            for file in self.torrent_dict[b'info'][b'files']:
                length += file[b'length']
        return length

    def start_download(self):
        logger.setLevel("INFO")
        logger.info(f"Start downloading '{self.file_name}'")
        create_if_not_exist(self.full_result_filepath)
        self.check_download_stat()
        self.downloaded_bytes = sum(self.received_pieces_mask) * self.piece_length
        threading.Thread(target=self.log_process).start()
        threading.Thread(target=self.process_download).start()

    def check_download_stat(self):
        with open(self.full_result_filepath, "r+b") as file:
            for i in range(self.pieces_cnt):
                file.seek(i * self.piece_length)
                piece = file.read(self.piece_length)
                if calculate_hash(piece) == self.pieces_hashes[i]:
                    self.received_pieces_mask[i] = 1
        logger.info(f"Pieces downloaded: '{sum(self.received_pieces_mask)}/{self.pieces_cnt}'")

    def log_process(self):
        while True:
            time.sleep(self.log_update_time)
            percent = (self.downloaded_bytes / self.full_length) * 100
            print("Downloading file: {0:} ({1:.2f} %), peers: {2}".format(self.file_name, percent,
                                                                          len(self.connected_peers)), end='\r')

    def process_download(self):
        if sum(self.received_pieces_mask) == self.pieces_cnt:
            raise Exception("File is already downloaded. Terminating the process of downloading")
        peers_ips = self.get_peers_ips()
        seed_cnt = len(peers_ips)
        if seed_cnt == 0:
            raise Exception("No available peers for download!")
        for peer in peers_ips:
            try:
                if self.downloaded_bytes != self.full_length:
                    threading.Thread(target=self.connect_to_peer, args=(peer,), daemon=True).start()
                    time.sleep(5)
                else:
                    break
            except Exception as ex:
                print(f"Download was terminated due exception")
                raise ex

    def create_tracker_params(self):
        return {'info_hash': self.info_hash,
                'peer_id': self.peer_id,
                'port': PORT,
                'event': 'started',
                'uploaded': '0',
                'downloaded': '0',
                'left': str(self.full_length),
                'compact': '1'}

    def get_peers_ips(self):
        root_url = self.torrent_dict[b'announce'].decode()
        if validators.url(root_url):
            request_params = self.create_tracker_params()
            response = make_get_request(root_url, request_params)
            if response.code == 200:
                response_body = response.read()
                if b'failure reason' not in response_body:
                    response_dict = bencodepy.decode(response_body)
                    return calculate_peers(response_dict[b'peers'])
                else:
                    raise Exception("Failure connecting to Tracker")
            else:
                raise Exception("Can't connect to Tracker")

        else:
            raise Exception("Not valid tracker url in torrent file.\n"
                            "Can't connect to Tracker!")

    def connect_to_peer(self, peer):
        handshake = calculate_handshake(self.info_hash, self.peer_id)
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            peer_socket.connect(peer)
            peer_socket.sendall(handshake)
            peer_socket.settimeout(5)
            interested_response = peer_socket.recv(len(handshake))
            if interested_response:
                if self.info_hash == struct.unpack('!20s', interested_response[28:48])[0]:
                    with self.log_lock:
                        self.connected_peers.append(peer)
                    peer_socket.settimeout(None)
                    self.download_data(peer_socket)
                else:
                    peer_socket.close()
        except (socket.timeout, OSError):
            print("Timeout while connecting to peer")
        peer_socket.close()
        with self.log_lock:
            if peer in self.connected_peers:
                self.connected_peers.remove(peer)

    def download_data(self, peer_socket):
        peer_socket.settimeout(5)
        bitfield = []
        my_pieces = {}
        try:
            while True:
                msg_length = get_message_length(peer_socket)
                if msg_length == 0:
                    continue
                data = get_data(msg_length, peer_socket)
                peer_socket.settimeout(None)
                is_choked = self.process_data(data, bitfield, my_pieces, peer_socket)
                if is_choked:
                    if sum(self.received_pieces_mask) != self.pieces_cnt:
                        self.restore_info(my_pieces)
                    break
        except (socket.timeout, OSError):
            self.restore_info(my_pieces)

    # в случае ошибки при скачивании, все неполные pieces удаляются
    def restore_info(self, my_pieces):
        for index, piece in my_pieces.items():
            if len(piece) < self.piece_length:
                try:
                    self.pieces_lock.acquire()
                    self.received_pieces_mask[index] = 0
                except Exception as ex:
                    print(ex)
                    pass
                finally:
                    self.pieces_lock.release()

    def process_data(self, raw_data, bitfield, my_pieces, peer_socket):
        try:
            msg_type = struct.unpack('!B', raw_data[:1])[0]
            if msg_type == MsgType.Bitfield.value:
                bitfield += get_bitfield_data(raw_data)
                with self.pieces_lock:
                    if is_interested(bitfield, self.received_pieces_mask):
                        send_interested_msg(peer_socket)
                    else:
                        send_not_interested_msg(peer_socket)
                        return True

            elif msg_type == MsgType.Have.value:
                set_have(raw_data, bitfield)
                return False

            elif msg_type == MsgType.Unchoke.value:
                if bitfield:
                    self.send_request_for_next_piece(bitfield, peer_socket, my_pieces)
                return False

            elif msg_type == MsgType.Choke.value:
                return True

            elif msg_type == MsgType.Piece.value:
                return self.process_piece(raw_data[1:], my_pieces, peer_socket, bitfield)
            return False
        except Exception as ex:
            print(ex)
            return True

    def process_piece(self, raw_data, my_pieces, peer_socket, bitfield):
        try:
            index, begin = struct.unpack('!II', raw_data[:8])
            block = raw_data[8:]
            my_pieces[index] += block
            prev_bytes = self.piece_length * index
            # либо мы полностью получили piece, либо piece - предпоследний и при этом полученный блок полностью его заполнил
            if len(my_pieces[index]) == self.piece_length or (
                    (index == self.pieces_cnt - 1 and prev_bytes + len(my_pieces[index]) == self.full_length)):
                if calculate_hash(my_pieces[index]) == self.pieces_hashes[index]:
                    self.write_piece_to_file(my_pieces[index], index)
                    # если не все pieces загрузили делаем запрос на следующий
                    if sum(self.received_pieces_mask) != self.pieces_cnt:
                        self.send_request_for_next_piece(bitfield, peer_socket, my_pieces)
                        return False
                    else:
                        return True
                # если hash полученного piece не совпал, то пишем что данный piece не был загружен
                else:
                    with self.pieces_lock:
                        self.received_pieces_mask[index] = 0
                    print("hash wasn't correct ")
                    return True
            else:
                # если предпоследний piece и текущий блок его не заполнил, а следующий блок его переполнит, то делаем
                # запрос на блок нового размера
                if index == self.pieces_cnt - 1 and prev_bytes + len(my_pieces[index]) + BLOCK_SIZE > self.full_length:
                    last_piece_size = self.full_length - prev_bytes - len(my_pieces[index])
                    if last_piece_size == 0: return False
                    send_block_request(peer_socket=peer_socket, index=index, length=last_piece_size,
                                       begin=len(my_pieces[index]))
                # случай, когда наш piece еще не до конца заполнился, делаем запрос на стандартный размер блока
                else:
                    send_block_request(peer_socket=peer_socket, index=index, length=BLOCK_SIZE,
                                       begin=len(my_pieces[index]))
                return False
        except Exception as ex:
            print(ex)
            return True

    def write_piece_to_file(self, block, index):
        new_bytes = len(block)
        begin = self.piece_length * index
        with open(self.full_result_filepath, "r+b") as cur_file:
            cur_file.seek(begin)
            cur_file.write(block)
        with self.log_lock:
            self.downloaded_bytes += new_bytes

    def send_request_for_next_piece(self, bitfield, peer_socket, my_pieces):
        for i in range(len(bitfield)):
            if bitfield[i] == 1:
                try:
                    self.pieces_lock.acquire()
                    if self.received_pieces_mask[i] == 0:
                        self.received_pieces_mask[i] = 1
                        self.pieces_lock.release()
                        my_pieces[i] = b''
                        send_block_request(peer_socket=peer_socket, index=i, length=BLOCK_SIZE)
                        break
                    else:
                        self.pieces_lock.release()
                except Exception as ex:
                    print(ex)
                    break


if __name__ == '__main__':
    client = BitTorrentClient()
    client.start_download()
