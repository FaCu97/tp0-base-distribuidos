import socket
import logging


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
            # TODO: Modify the receive to avoid short-reads
            msg = client_sock.recv(1024).rstrip().decode('utf-8')
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(msg).encode('utf-8'))
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
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
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._is_shutting_down:
                logging.info('action: accept_connections | result: fail')
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
