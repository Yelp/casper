import socket


def listen():
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the socket to the port
    server_address = ('0.0.0.0', 1234)
    sock.bind(server_address)

    with open('/var/log/metrics/metrics.log', 'w') as fp:
        while True:
            data = sock.recv(4096)
            fp.write('{}\n'.format(data.decode('utf-8')))
            fp.flush()


if __name__ == '__main__':
    listen()