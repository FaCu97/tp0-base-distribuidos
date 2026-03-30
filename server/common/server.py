import socket
import logging
import datetime

from common.utils import Bet, store_bets


NAME_FIELD_SIZE = 30
SURNAME_FIELD_SIZE = 30
DOCUMENT_FIELD_SIZE = 8
BIRTH_FIELD_SIZE = 10
NUMBER_FIELD_SIZE = 4
BET_COUNT_SIZE = 2
BET_FRAME_SIZE = (
    NAME_FIELD_SIZE
    + SURNAME_FIELD_SIZE
    + DOCUMENT_FIELD_SIZE
    + BIRTH_FIELD_SIZE
    + NUMBER_FIELD_SIZE
)
MAX_BATCH_BYTES = 8 * 1024
ACK_OK = b"OK"
ACK_ERR = b"ER"


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._is_shutting_down = False
        self._active_client_sockets = set()

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        try:
            while not self._is_shutting_down:
                client_sock = self.__accept_new_connection()
                if client_sock is None:
                    continue
                self._active_client_sockets.add(client_sock)
                self.__handle_client_connection(client_sock)
        finally:
            self.__close_all_active_client_sockets()
        self._is_shutting_down = True
        self.__close_socket(self._server_socket, 'server_socket')

    def graceful_shutdown(self):
        self._is_shutting_down = True
        self.__close_socket(self._server_socket, 'server_socket')
        self.__close_all_active_client_sockets()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            agency = self.__infer_agency_from_client_socket(addr)

            while True:
                try:
                    bet_count = self.__read_bet_count(client_sock)
                except EOFError:
                    break

                payload = self.__read_batch_payload(client_sock, bet_count)
                decoded_bets = self.__decode_batch(payload, bet_count)

                to_store = []
                for bet in decoded_bets:
                    to_store.append(
                        Bet(
                            agency,
                            bet['nombre'],
                            bet['apellido'],
                            bet['documento'],
                            bet['nacimiento'],
                            str(bet['numero']),
                        )
                    )
                store_bets(to_store)

                logging.info('action: apuesta_recibida | result: success | cantidad: %s', bet_count)
                self.__send_ack(client_sock, ACK_OK)
        except OSError as e:
            logging.error('action: receive_message | result: fail | error: %s', e)
            self.__safe_send_error_ack(client_sock)
        except ValueError as e:
            logging.error('action: receive_message | result: fail | error: %s', e)
            if 'bet_count' in locals():
                logging.info('action: apuesta_recibida | result: fail | cantidad: %s', bet_count)
            self.__safe_send_error_ack(client_sock)
        finally:
            self._active_client_sockets.discard(client_sock)
            self.__close_socket(client_sock, 'client_socket')

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        try:
            c, addr = self._server_socket.accept()
            logging.info('action: accept_connections | result: success | ip: %s', addr[0])
            return c
        except OSError as e:
            if self._is_shutting_down:
                logging.info('action: accept_connections | result: success | reason: interrupted_by_shutdown')
                return None

            logging.error('action: accept_connections | result: fail | error: %s', e)
            raise

    def __close_socket(self, sock, resource_name):
        if sock is None:
            return

        try:
            sock.close()
            logging.info('action: close_resource | result: success | resource: %s', resource_name)
        except OSError as e:
            logging.error(
                'action: close_resource | result: fail | resource: %s | error: %s',
                resource_name,
                e,
            )

    def __close_all_active_client_sockets(self):
        for client_sock in list(self._active_client_sockets):
            self.__close_socket(client_sock, 'client_socket')
            self._active_client_sockets.discard(client_sock)

    def __read_exact(self, client_sock, size):
        data = bytearray()
        while len(data) < size:
            chunk = client_sock.recv(size - len(data))
            if not chunk:
                if len(data) == 0:
                    raise EOFError('connection closed by peer')
                raise OSError('connection closed before full frame reception')
            data.extend(chunk)

        return bytes(data)

    def __send_ack(self, client_sock, ack):
        client_sock.sendall(ack)

    def __read_bet_count(self, client_sock):
        raw_count = self.__read_exact(client_sock, BET_COUNT_SIZE)
        bet_count = int.from_bytes(raw_count, byteorder='big', signed=False)
        if bet_count <= 0:
            raise ValueError('bet_count must be greater than zero')
        return bet_count

    def __read_batch_payload(self, client_sock, bet_count):
        payload_size = bet_count * BET_FRAME_SIZE
        total_size = BET_COUNT_SIZE + payload_size
        if total_size > MAX_BATCH_BYTES:
            raise ValueError('batch payload exceeds 8kB limit')

        return self.__read_exact(client_sock, payload_size)

    def __safe_send_error_ack(self, client_sock):
        try:
            self.__send_ack(client_sock, ACK_ERR)
        except OSError:
            pass

    def __decode_bet_frame(self, frame):
        if len(frame) != BET_FRAME_SIZE:
            raise ValueError('invalid bet frame size')

        offset = 0
        nombre = frame[offset:offset + NAME_FIELD_SIZE].decode('utf-8').rstrip(' ')
        offset += NAME_FIELD_SIZE

        apellido = frame[offset:offset + SURNAME_FIELD_SIZE].decode('utf-8').rstrip(' ')
        offset += SURNAME_FIELD_SIZE

        documento = frame[offset:offset + DOCUMENT_FIELD_SIZE].decode('utf-8')
        offset += DOCUMENT_FIELD_SIZE

        nacimiento = frame[offset:offset + BIRTH_FIELD_SIZE].decode('utf-8')
        offset += BIRTH_FIELD_SIZE

        numero = int.from_bytes(frame[offset:offset + NUMBER_FIELD_SIZE], byteorder='big', signed=False)

        if not documento.isdigit():
            raise ValueError('documento must contain only digits')

        datetime.date.fromisoformat(nacimiento)

        return {
            'nombre': nombre,
            'apellido': apellido,
            'documento': documento,
            'nacimiento': nacimiento,
            'numero': numero,
        }

    def __decode_batch(self, payload, bet_count):
        expected_size = bet_count * BET_FRAME_SIZE
        if len(payload) != expected_size:
            raise ValueError('invalid batch payload size')

        bets = []
        offset = 0
        for _ in range(bet_count):
            frame = payload[offset:offset + BET_FRAME_SIZE]
            bets.append(self.__decode_bet_frame(frame))
            offset += BET_FRAME_SIZE

        return bets

    def __infer_agency_from_client_socket(self, addr):
        _ = addr
        return '0'
