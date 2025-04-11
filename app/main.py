import socket  # noqa: F401
import argparse
import logging
from .redis_server import RedisServer


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Configure the root logger
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Direct all logs to the console
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Simple Redis server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dir", type=str, help="The directory of the RDB file")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument("--port", type=int, help="The port to run on.", default=6379)
    parser.add_argument(
        "--replicaof",
        type=str,
        help="Replica server, master host and port.",
        default="master",
    )
    args = parser.parse_args()
    redis_port = args.port

    if args.replicaof == "master":
        role = "master"
        master_host = None
        master_port = None
        logger.debug("Master server")
    else:
        role = "slave"
        master_host, master_port = args.replicaof.split()
        logger.debug(f"Replica server: {master_host=}, {master_port=}")
        perform_handshake_with_master(redis_port, master_host, int(master_port))

    server_socket = socket.create_server(("localhost", redis_port), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()  # wait for a client.
        redis_server = RedisServer(connection, role)
        redis_server.start()


def perform_handshake_with_master(redis_port: int, master_host: str, master_port: int):
    master_socket = socket.create_connection((master_host, master_port))
    fm_socket = master_socket.makefile("rwb")

    # Handshake Part-1: replica sends a PING to the master.
    fm_socket.write("*1\r\n$4\r\nPING\r\n".encode("utf-8"))
    fm_socket.flush()
    response = fm_socket.readline().rstrip(b"\r\n")
    if response != b"+PONG":
        raise Exception("Ping failed")

    # Handshake Part-2: replica sends REPLCONF twice to the master.
    fm_socket.write(
        f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n"
        f"${len(str(redis_port))}\r\n{redis_port}\r\n".encode("utf-8")
    )
    fm_socket.flush()
    response = fm_socket.readline().rstrip(b"\r\n")
    if response != b"+OK":
        raise Exception("Reply failed")
    fm_socket.write(
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode("utf-8")
    )
    fm_socket.flush()
    response = fm_socket.readline().rstrip(b"\r\n")
    if response != b"+OK":
        raise Exception("Reply failed")

    # Handshake Part-3: replica sends PSYNC with replication ID and offset of the master.from
    fm_socket.write(
        "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode("utf-8")
    )
    fm_socket.flush()


if __name__ == "__main__":
    main()
