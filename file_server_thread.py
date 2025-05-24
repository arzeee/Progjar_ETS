# file_server_thread.py
import socket
import logging
from concurrent.futures import ThreadPoolExecutor
from file_protocol import FileProtocol

fp = FileProtocol()

def ProcessTheClient(connection, address):
    buffer = ''
    try:
        while True:
            data = connection.recv(1024)
            if data:
                buffer += data.decode()
                logging.warning(f"Buffer from {address}: {buffer}")

                if '\r\n\r\n' in buffer:
                    request = buffer.strip().split('\r\n\r\n')[0]
                    logging.warning(f"Processing request from {address}: {request}")
                    hasil = fp.proses_string(request)
                    hasil = hasil + "\r\n\r\n"
                    connection.sendall(hasil.encode())
                    buffer = ''
                    break
            else:
                logging.warning(f"Connection closed by {address}")
                break
    except OSError as e:
        logging.warning(f"OSError for {address}: {e}")
    finally:
        connection.close()
        logging.warning(f"Connection to {address} closed.")

def Server():
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    my_socket.bind(('0.0.0.0', 10001))
    my_socket.listen(5)

    logging.warning("Multithreaded server berjalan di 0.0.0.0:10001")

    with ThreadPoolExecutor(max_workers=20) as executor:
        try:
            while True:
                connection, client_address = my_socket.accept()
                logging.warning(f"Connection from {client_address}")
                executor.submit(ProcessTheClient, connection, client_address)
        except KeyboardInterrupt:
            logging.warning("Server stopped by KeyboardInterrupt")
        finally:
            my_socket.close()
            logging.warning("Server socket ditutup.")

def main():
    Server()

if __name__ == "__main__":
    main()

