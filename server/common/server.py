import socket
import logging
import datetime
import threading

from common.utils import Bet, store_bets, load_bets, has_won


NAME_FIELD_SIZE = 30
SURNAME_FIELD_SIZE = 30
DOCUMENT_FIELD_SIZE = 8
BIRTH_FIELD_SIZE = 10
NUMBER_FIELD_SIZE = 4
MESSAGE_TYPE_SIZE = 1
AGENCY_FIELD_SIZE = 1
BET_COUNT_SIZE = 2
BET_FRAME_SIZE = (
    NAME_FIELD_SIZE
    + SURNAME_FIELD_SIZE
    + DOCUMENT_FIELD_SIZE
    + BIRTH_FIELD_SIZE
    + NUMBER_FIELD_SIZE
)
MAX_BATCH_BYTES = 8 * 1024
WINNER_COUNT_SIZE = 2
MSG_BATCH = b"B"
MSG_DRAW_CONFIRM = b"N"
MSG_WINNERS_REQUEST = b"W"
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
        self._client_threads = set()
        self._connections_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._draw_ready = threading.Condition(self._state_lock)
        self._confirmed_agencies = set()
        self._draw_done = False
        self._winners_by_agency = {}
        self._total_agencies = listen_backlog

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
                self.__register_active_client_socket(client_sock)
                self.__start_client_thread(client_sock)
        finally:
            self.__close_all_active_client_sockets()
            self.__wait_all_client_threads()
        self._is_shutting_down = True
        self.__close_socket(self._server_socket, 'server_socket')

    def graceful_shutdown(self):
        self._is_shutting_down = True
        self.__close_socket(self._server_socket, 'server_socket')
        self.__close_all_active_client_sockets()
        self.__wait_all_client_threads()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            _ = addr

            while True:
                try:
                    message_type = self.__read_message_type(client_sock)
                except EOFError:
                    break

                if message_type == MSG_BATCH:
                    agency, bet_count = self.__read_batch_header(client_sock)
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

                    logging.info('action: apuesta_recibida | result: success | cantidad: %s | agency: %s', bet_count, agency)
                    self.__send_ack(client_sock, ACK_OK)
                    continue

                if message_type == MSG_DRAW_CONFIRM:
                    agency = self.__read_agency(client_sock)
                    self.__process_draw_confirmation(agency)
                    self.__send_ack(client_sock, ACK_OK)
                    continue

                if message_type == MSG_WINNERS_REQUEST:
                    agency = self.__read_agency(client_sock)
                    self.__send_winners_response(client_sock, agency)
                    continue

                raise ValueError('unknown message type')
        except OSError as e:
            logging.error('action: receive_message | result: fail | error: %s', e)
            self.__safe_send_error_ack(client_sock)
        except ValueError as e:
            logging.error('action: receive_message | result: fail | error: %s', e)
            if 'bet_count' in locals():
                logging.info('action: apuesta_recibida | result: fail | cantidad: %s', bet_count)
            self.__safe_send_error_ack(client_sock)
        finally:
            self.__unregister_active_client_socket(client_sock)
            self.__close_socket(client_sock, 'client_socket')
            self.__unregister_client_thread(threading.current_thread())

    def __start_client_thread(self, client_sock):
        worker = threading.Thread(target=self.__handle_client_connection, args=(client_sock,))
        with self._connections_lock:
            self._client_threads.add(worker)
        worker.start()

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
        with self._connections_lock:
            sockets = list(self._active_client_sockets)

        for client_sock in sockets:
            self.__close_socket(client_sock, 'client_socket')

    def __wait_all_client_threads(self):
        with self._connections_lock:
            threads = list(self._client_threads)

        for worker in threads:
            worker.join()

    def __register_active_client_socket(self, client_sock):
        with self._connections_lock:
            self._active_client_sockets.add(client_sock)

    def __unregister_active_client_socket(self, client_sock):
        with self._connections_lock:
            self._active_client_sockets.discard(client_sock)

    def __unregister_client_thread(self, worker):
        with self._connections_lock:
            self._client_threads.discard(worker)

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

    def __read_batch_header(self, client_sock):
        raw_header = self.__read_exact(client_sock, AGENCY_FIELD_SIZE + BET_COUNT_SIZE)
        agency = raw_header[0]
        raw_count = raw_header[AGENCY_FIELD_SIZE:AGENCY_FIELD_SIZE + BET_COUNT_SIZE]
        bet_count = int.from_bytes(raw_count, byteorder='big', signed=False)
        if bet_count <= 0:
            raise ValueError('bet_count must be greater than zero')
        return str(agency), bet_count

    def __read_message_type(self, client_sock):
        return self.__read_exact(client_sock, MESSAGE_TYPE_SIZE)

    def __read_agency(self, client_sock):
        raw_agency = self.__read_exact(client_sock, AGENCY_FIELD_SIZE)
        return str(raw_agency[0])

    def __read_batch_payload(self, client_sock, bet_count):
        payload_size = bet_count * BET_FRAME_SIZE
        total_size = MESSAGE_TYPE_SIZE + AGENCY_FIELD_SIZE + BET_COUNT_SIZE + payload_size
        if total_size > MAX_BATCH_BYTES:
            raise ValueError('batch payload exceeds 8kB limit')

        return self.__read_exact(client_sock, payload_size)

    def __safe_send_error_ack(self, client_sock):
        try:
            self.__send_ack(client_sock, ACK_ERR)
        except OSError:
            pass

    def __process_draw_confirmation(self, agency):
        should_run_draw = False
        with self._state_lock:
            self._confirmed_agencies.add(agency)
            if len(self._confirmed_agencies) == self._total_agencies and not self._draw_done:
                should_run_draw = True

        if should_run_draw:
            logging.info('action: running_draw')

            self.__run_draw()

    def __run_draw(self):
        winners_by_agency = {}
        try:
            for bet in load_bets():
                if has_won(bet):
                    #debug
                    logging.info('action: ganador_encontrado')

                    winners_by_agency.setdefault(str(bet.agency), []).append(bet.document)
        except FileNotFoundError:
            winners_by_agency = {}

        with self._state_lock:
            self._winners_by_agency = winners_by_agency
            self._draw_done = True
            self._draw_ready.notify_all()

        logging.info('action: sorteo | result: success')

    def __send_winners_response(self, client_sock, agency):
        with self._draw_ready:
            while not self._draw_done:
                self._draw_ready.wait()
            winners = list(self._winners_by_agency.get(agency, []))

        response = bytearray()
        response.extend(ACK_OK)
        response.extend(len(winners).to_bytes(WINNER_COUNT_SIZE, byteorder='big', signed=False))
        for document in winners:
            encoded_document = str(document).encode('utf-8')
            if len(encoded_document) > DOCUMENT_FIELD_SIZE:
                encoded_document = encoded_document[:DOCUMENT_FIELD_SIZE]
            response.extend(encoded_document.ljust(DOCUMENT_FIELD_SIZE, b' '))

        client_sock.sendall(bytes(response))

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

