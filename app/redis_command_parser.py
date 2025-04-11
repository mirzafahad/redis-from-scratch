import logging
from typing import IO, Any
from enum import StrEnum, auto
from .exceptions import RedisSocketReadError, RedisCommandError


class UpperCaseStrEnum(StrEnum):
    # Subclass of string enum that saves values with uppercase values
    @staticmethod
    def _generate_next_value_(name: str, *args: Any) -> str:
        return name.upper()


class RedisCommand(UpperCaseStrEnum):
    PING = auto()
    ECHO = auto()
    SET = auto()
    GET = auto()
    DELETE = auto()
    INFO = auto()
    REPLCONF = auto()
    PSYNC = auto()


class RedisCommandParser(object):
    def __init__(self, file_socket: IO[bytes]):
        self.logger = logging.getLogger(__name__)
        self.file_socket = file_socket

    def get_redis_command(self) -> [RedisCommand, list]:
        redis_packet = self._read_raw_redis_packet()
        command = RedisCommand(redis_packet[0])
        args = redis_packet[1:]
        return command, args

    def _read_raw_redis_packet(self) -> list[str]:
        """
        Handles redis packets. The packet is structured according to the following protocols:
        https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description

        Raises:
            RedisCommandError during command errors.
        """
        # The first byte defines the data type.
        data_type = self._read_char_from_socket()

        match data_type:
            case "-":
                """
                Handle an error message.
                Format: -Error message\r\n
                Example: -ERR unknown command 'asdf'\r\n
                """
                error_msg = self.file_socket.readline().decode("utf-8").rstrip("\r\n")
                raise RedisCommandError(error_msg)

            case "*":
                """
                Handle an array.
                Except error, all redis packets come as an array.

                Format: *<number of elements>\r\n<0 or more of $>\r\n
                Example:
                    SET: *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nHello\r\n
                    DEL: *2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n
                    FLUSHALL: *1\r\n$8\r\nFLUSHALL\r\n
                    ECHO: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
                    PING: *1\r\n$4\r\nPING\r\n
                """
                num_elements = int(self.file_socket.readline().rstrip(b"\r\n"))
                return [self._read_bulk_string() for _ in range(num_elements)]

            case _:
                raise RedisCommandError(f"Invalid data type: {data_type}")

    def _read_char_from_socket(self) -> str:
        """
        Read one byte from the socket, convert it to character and return that.

        Returns:
             One string character.
        """
        char = self.file_socket.read(1).decode("utf-8")
        if not char:
            raise RedisSocketReadError("Read byte failed, empty byte.")

        return char

    def _read_bulk_string(self) -> str:
        """
        Read bulk string from the socket.
        Format: $<length>\r\n<length of bytes>\r\n

        Note: Bulk string is a type in Redis protocol.
        """
        data_type = self._read_char_from_socket()
        if data_type != "$":
            raise RedisCommandError(f"Invalid data type: {data_type}. Expected: $")

        # First read the length (<length>\r\n).
        length = int(self.file_socket.readline().rstrip(b"\r\n"))
        length += 2  # Include the trailing \r\n in count.

        # Return just the string, no length, no \r\n.
        return self.file_socket.read(length)[:-2].decode("utf-8")
