from dataclasses import dataclass
from socket import socket
import logging


@dataclass
class Error:
    message: str


class CommandError(Exception):
    pass


class RedisCommandParser(object):
    def __init__(self, socket_conn: socket):
        self.logger = logging.getLogger(__name__)

        # Convert socket_conn (a socket object) into a file-like object.
        self.fsocket = socket_conn.makefile("rwb")

        self._data_handlers = {
            "+": self._handle_simple_string,
            "-": self._handle_error,
            ":": self._handle_integer,
            "$": self._handle_bulk_string,
            "*": self._handle_array,
            "%": self._handle_dict,
        }
        self._command_handlers = {
            "GET": self._handle_command_get,
            "SET": self._handle_command_set,
            "DELETE": 3,
            "FLUSH": 4,
            "MGET": 5,
            "MSET": 6,
            "PING": self._handle_command_ping,
            "ECHO": self._handle_command_echo,
        }

        # This is the in-memory key-value storage for redis.
        self._redis_storage = {}

    def parse_command(self):
        """
        First parse the request received from the client, and get the data.
        Then take action and prepare response to send to the client.
        :return:
        """
        data = self._parse_resp_input_data()
        command = data[0]
        return self._command_handlers[command](data[1:])

    def _parse_resp_input_data(self):
        """
        Parse according to this protocol:
        https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description
        """
        # The first byte defines the data type.
        data_type = self.fsocket.read(1).decode("utf-8")
        if not data_type:
            raise CommandError("Empty data type / first byte.")

        try:
            # Delegate to the appropriate handler based on the first byte.
            return self._data_handlers[data_type]()
        except KeyError:
            raise CommandError(f"Unsupported data type: {data_type}")

    def _handle_command_ping(self, data: list) -> bytes:
        """
        We will receive: *1\r\n$4\r\nPING\r\n
        """
        self.logger.debug(f"Received {data[0]} command.")
        return b"+PONG\r\n"

    def _handle_command_echo(self, data: list) -> bytes:
        """
        The ECHO command: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
        Method will receive: hey
        The response should be: $3\r\nhey\r\n
        """
        self.logger.debug("Received ECHO command.")
        echo_val = f"${len(data[0])}\r\n{data[0]}\r\n".encode("utf-8")
        return echo_val

    def _handle_command_set(self, data: list) -> bytes:
        # The SET command: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        # Method will receive: ["foo", "bar"]
        # The response should be: +OK\r\n
        self.logger.debug("Received SET command.")
        key, value = data
        self._redis_storage[key] = value
        return b"+OK\r\n"

    def _handle_command_get(self, data: list) -> bytes:
        # The SET command: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
        # Method will receive: ["foo"]
        # The response should be: $3\r\nbar\r\n
        self.logger.debug("Received GET command.")

        value = self._redis_storage.get(data[0], None)
        if value is None:
            return b"$-1\r\n"
        return f"${len(value)}\r\n{value}\r\n".encode("utf-8")

    def _handle_simple_string(self):
        return self.fsocket.readline().decode("utf-8").rstrip("\r\n")

    def _handle_error(self):
        error_msg = self.fsocket.readline().decode("utf-8").rstrip("\r\n")
        return Error(error_msg)

    def _handle_integer(self):
        return int(self.fsocket.readline().rstrip(b"\r\n"))

    def _handle_bulk_string(self):
        """
        Format: $<length>\r\n<data>\r\n
        """
        # First read the length ($<length>\r\n).
        length = int(self.fsocket.readline().rstrip(b"\r\n"))
        length += 2  # Include the trailing \r\n in count.
        return self.fsocket.read(length)[:-2].decode("utf-8")

    def _handle_null(self):
        pass

    def _handle_array(self):
        """
        *<number of elements>\r\n<0 or more of above>\r\n
        """
        num_elements = int(self.fsocket.readline().rstrip(b"\r\n"))
        return [self._parse_resp_input_data() for _ in range(num_elements)]

    def _handle_dict(self):
        num_items = int(self.fsocket.readline().rstrip(b"\r\n"))
        elements = [self._parse_resp_input_data() for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
