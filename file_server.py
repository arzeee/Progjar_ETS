import socket
import os
import argparse
import logging
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

HOST = '0.0.0.0'
PORT = 10001
BUFFER_SIZE = 524288     # 1MB buffer
STORAGE_DIR = 'storage'
LOG_FILE = 'server.log'

os.makedirs(STORAGE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

def send_json_response(conn, status, data):
    resp = json.dumps({"status": status, "data": data}) + "\r\n\r\n"
    logging.debug(f"Sending response: status={status}, data={str(data)[:100]}")
    conn.sendall(resp.encode())

def is_valid_filename(filename):
    import re
    valid = re.match(r'^[\w\-.]+$', filename) is not None
    logging.debug(f"Validating filename '{filename}': {valid}")
    return valid

def handle_client_raw(conn, addr):
    try:
        thread_name = threading.current_thread().name
        logging.info(f"[{thread_name}] Connection from {addr}")

        header_data = b""
        while b"\r\n\r\n" not in header_data:
            chunk = conn.recv(BUFFER_SIZE)
            if not chunk:
                logging.warning(f"[{thread_name}] Client {addr} disconnected before sending full header")
                return
            header_data += chunk

        header_text = header_data.decode(errors='replace').strip()
        parts = header_text.split()
        if len(parts) < 3:
            send_json_response(conn, "ERROR", "Invalid command format")
            return

        command = parts[0].upper()
        filename = parts[1]
        if not is_valid_filename(filename):
            send_json_response(conn, "ERROR", "Invalid filename")
            return

        try:
            filesize = int(parts[2])
        except ValueError:
            send_json_response(conn, "ERROR", "Invalid file size")
            return

        filepath = os.path.join(STORAGE_DIR, filename)

        if command == "UPLOAD":
            header_end = header_data.find(b"\r\n\r\n") + 4
            file_data = header_data[header_end:]
            remaining = filesize - len(file_data)

            if remaining < 0:
                send_json_response(conn, "ERROR", "File size smaller than received data")
                return

            with open(filepath, 'wb') as f:
                f.write(file_data)
                while remaining > 0:
                    chunk = conn.recv(min(BUFFER_SIZE, remaining))
                    if not chunk:
                        break
                    f.write(chunk)
                    remaining -= len(chunk)

            if remaining > 0:
                send_json_response(conn, "ERROR", "Incomplete file received")
            else:
                send_json_response(conn, "OK", f"Uploaded {filename}")

        elif command == "GET":
            if not os.path.exists(filepath):
                send_json_response(conn, "ERROR", "File not found")
                return

            filesize = os.path.getsize(filepath)
            send_json_response(conn, "OK", {"filename": filename, "filesize": filesize})

            with open(filepath, 'rb') as f:
                while True:
                    chunk = f.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    conn.sendall(chunk)

        else:
            send_json_response(conn, "ERROR", "Invalid command")

    except Exception as e:
        logging.error(f"Exception handling client {addr}: {e}", exc_info=True)
        try:
            send_json_response(conn, "ERROR", str(e))
        except:
            pass
    finally:
        conn.close()
        logging.info(f"Closed connection from {addr}")

def handle_client(conn, addr):
    handle_client_raw(conn, addr)

def start_server_single():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        logging.info(f"Single-threaded server started on {HOST}:{PORT}")

        while True:
            conn, addr = server_socket.accept()
            handle_client(conn, addr)

def start_server_threaded(workers):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        logging.info(f"Thread-pool server started on {HOST}:{PORT} with {workers} workers")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            while True:
                conn, addr = server_socket.accept()
                executor.submit(handle_client, conn, addr)

def start_server_process(workers):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        logging.info(f"Process-pool server started on {HOST}:{PORT} with {workers} workers")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            while True:
                conn, addr = server_socket.accept()
                # NOTE: Connection object cannot be shared across processes.
                # Workaround: Use multiprocessing queue/pipes or do synchronous fallback
                # So we handle the connection synchronously in the main process
                # but you can offload processing-only task to process pool if needed
                executor.submit(handle_client_raw, conn, addr)

def main():
    parser = argparse.ArgumentParser(description="File server with multiple concurrency modes.")
    parser.add_argument('--mode', choices=['single', 'thread', 'process'], default='single', help="Mode to run the server")
    parser.add_argument('--workers', type=int, default=1, help="Number of worker threads or processes")
    args = parser.parse_args()

    if args.mode == 'single':
        start_server_single()
    elif args.mode == 'thread':
        start_server_threaded(args.workers)
    elif args.mode == 'process':
        start_server_process(args.workers)
    else:
        logging.error("Invalid mode selected.")

if __name__ == '__main__':
    main()
