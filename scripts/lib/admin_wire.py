#!/usr/bin/env python3
import socket
import struct
import sys
import time


def cstring(value):
    return value.encode("utf-8") + b"\x00"


def startup_packet(user, database):
    body = (
        struct.pack("!I", 196608)
        + cstring("user")
        + cstring(user)
        + cstring("database")
        + cstring(database)
        + b"\x00"
    )
    return struct.pack("!I", len(body) + 4) + body


def query_packet(sql):
    body = cstring(sql)
    return b"Q" + struct.pack("!I", len(body) + 4) + body


def terminate_packet():
    return b"X" + struct.pack("!I", 4)


def read_response(sock, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    chunks = []
    quiet_since = None
    sock.setblocking(False)

    while time.monotonic() < deadline:
        try:
            chunk = sock.recv(4096)
        except BlockingIOError:
            chunk = None

        if chunk:
            chunks.append(chunk)
            quiet_since = None
            continue

        if chunk == b"":
            break

        if chunks:
            if quiet_since is None:
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= 0.15:
                break

        time.sleep(0.05)

    response = b"".join(chunks)
    return "".join(
        chr(byte) if byte in (9, 10, 13) or 32 <= byte < 127 else " "
        for byte in response
    )


def main():
    if len(sys.argv) != 5:
        print("usage: admin_wire.py <port> <user> <database> <sql>", file=sys.stderr)
        return 2

    port = int(sys.argv[1])
    user = sys.argv[2]
    database = sys.argv[3]
    sql = sys.argv[4]

    with socket.create_connection(("127.0.0.1", port), timeout=3.0) as sock:
        sock.sendall(startup_packet(user, database))
        read_response(sock, 3.0)
        sock.sendall(query_packet(sql))
        response = read_response(sock, 3.0)
        try:
            sock.sendall(terminate_packet())
        except OSError:
            pass

    print(response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
