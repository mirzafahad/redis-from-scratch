import socket  # noqa: F401
import threading
from redis_command_parser import RedisCommandParser


def main():

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()  # wait for a client.
        client_handler = threading.Thread(target=redis_server, args=(connection,))
        client_handler.start()


def redis_server(connection):
    redis_command_parser = RedisCommandParser(connection)
    while connection:
        response = redis_command_parser.parse_command()
        connection.sendall(response)


if __name__ == "__main__":
    main()
