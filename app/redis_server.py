import socket
import threading
import logging
import time
from .data import ServerRole, ServerConfiguration, ServerInfo, RedisData
from .exceptions import RedisCommandError
from .redis_command_parser import RedisCommandParser, RedisCommand


class RedisServer(threading.Thread):
    def __init__(self, socket_conn: socket, role: str):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.socket_conn = socket_conn
        # Convert socket_conn (a socket object) into a file-like object.
        self.fsocket = socket_conn.makefile("rwb")

        server_info = ServerInfo(role=ServerRole(role))
        self.server_conf = ServerConfiguration(info=server_info)

        # This is the in-memory key-value storage for redis.
        self._redis_storage = {}

        # if role == "slave":
        #     print(f"Performing handshake on {master_host}:{master_port}")
        #     self._perform_handshake_with_master(master_host, master_port)

        self.command_parser = RedisCommandParser(self.fsocket)

    def run(self):
        try:
            while True:
                redis_cmd, args = self.command_parser.get_redis_command()
                self.execute_command(redis_cmd, args)
        except (ConnectionResetError, BrokenPipeError):
            print("Connection closed by the remote host")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.fsocket.close()
            self.socket_conn.close()

    @staticmethod
    def _perform_handshake_with_master(master_host: str, master_port: int):
        master_socket = socket.create_connection((master_host, master_port))
        master_socket.sendall(str.encode("*1\r\n$4\r\nPING\r\n"))

    def execute_command(self, redis_cmd: RedisCommand, cmd_args: list):
        match redis_cmd:
            case RedisCommand.PING:
                """
                We will receive: *1\r\n$4\r\nPING\r\n
                """
                self._send_response("+PONG\r\n")

            case RedisCommand.ECHO:
                """
                ECHO command example: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
                The response should be: $3\r\nhey\r\n
                """
                echo_response = self._generate_bulk_string(cmd_args[0])
                self._send_response(echo_response)

            case RedisCommand.SET:
                """
                The SET command: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
                The response should be: +OK\r\n
                """
                if len(cmd_args) == 2:
                    key, value = cmd_args
                    expire_at_s = None
                elif len(cmd_args) == 4:
                    key, value, expiry_cmd, expire_in_ms_str = cmd_args
                    if expiry_cmd.upper() != "PX":
                        raise RedisCommandError("The argument for expiring the key is PX.")
                    expire_at_s = time.time() + (int(expire_in_ms_str) / 1000)
                else:
                    raise RedisCommandError(f"SET doesn't support the command")

                self._redis_storage[key] = RedisData(value=value, expire_at=expire_at_s)
                self._send_response("+OK\r\n")

            case RedisCommand.GET:
                # The response should be: $5\r\nValue\r\n
                redis_data = self._redis_storage.get(cmd_args[0])

                if redis_data is None:
                    self._send_response("$-1\r\n")
                    return

                if redis_data.expire_at is not None and time.time() > redis_data.expire_at:
                    self._send_response("$-1\r\n")
                    return

                response = self._generate_bulk_string(f"{redis_data.value}")
                self._send_response(response)

            case RedisCommand.DELETE:
                pass

            case RedisCommand.INFO:
                if cmd_args[0] == "replication":
                    ret_val = (
                        f"role:{self.server_conf.info.role}\r\n"
                        f"master_replid:{self.server_conf.info.master_replid}\r\n"
                        f"master_repl_offset:{self.server_conf.info.master_repl_offset}\r\n"
                    )
                    response = self._generate_bulk_string(ret_val)
                    self._send_response(response)

            case RedisCommand.REPLCONF:
                self._send_response("+OK\r\n")

            case RedisCommand.PSYNC:
                mid = self.server_conf.info.master_replid
                offset = self.server_conf.info.master_repl_offset
                response = self._generate_bulk_string(f"+FULLRESYNC {mid} {offset}")
                self._send_response(response)

                # Now send an empty RDB file.
                # Note: The hex byte is provided by the code-crafters and represents an empty RDB file content.
                rdb_file_hex = ("524544495330303131fa0972656469732d76657205372e322e30fa0a7265646973"
                                "2d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c410"
                                "00fa08616f662d62617365c000fff06e3bfec0ff5aa2")
                rdb_empty_file = bytes.fromhex(rdb_file_hex)
                rdb_file_response = f"${len(rdb_empty_file)}\r\n".encode() + rdb_empty_file
                self._send_response(rdb_file_response)

            case _:
                raise RedisCommandError(f"Received wrong command: {redis_cmd}")

    def _send_response(self, response: str | bytes):
        if isinstance(response, str):
            response = response.encode("utf-8")

        self.fsocket.write(response)
        self.fsocket.flush()

    @staticmethod
    def _generate_bulk_string(string: str | None) -> str:
        if string is None:
            return "$-1\r\n"

        return f"${len(string)}\r\n{string}\r\n"

    def _encode_array(self, array: list) -> bytes:
        bulk_strings = []
        for string in array:
            bulk_strings.append(self._encode_bulk_string(string).decode("utf-8"))

        return f"*{len(array)}\r\n{''.join(bulk_strings)}".encode("utf-8")

    def _handle_integer(self):
        # return int(self.fsocket.readline().rstrip(b"\r\n"))
        pass

    def _handle_null(self):
        pass

    def _handle_dict(self):
        # num_items = int(self.fsocket.readline().rstrip(b"\r\n"))
        # elements = [self._parse_resp_input_data() for _ in range(num_items * 2)]
        # return dict(zip(elements[::2], elements[1::2]))
        pass
