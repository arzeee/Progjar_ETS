import os
import socket
import json
import base64
import logging
import argparse
import time
import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def recv_until(sock, delimiter=b"\r\n\r\n"):
    """Terima data dari socket sampai delimiter ditemukan."""
    buffer = b""
    while delimiter not in buffer:
        data = sock.recv(524288)
        if not data:
            break
        buffer += data
    if delimiter in buffer:
        header_part, rest = buffer.split(delimiter, 1)
        return header_part.decode(), rest
    else:
        return buffer.decode(), b""

def send_command(server_ip, server_port, command_str="", file_data_bytes=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((server_ip, server_port))

        # Kirim command string + akhir baris pemisah
        sock.sendall((command_str + "\r\n\r\n").encode())

        # Jika ada file_data_bytes, kirim dalam chunk
        if file_data_bytes:
            chunk_size = 524288     # 1 mb per chunk
            total_sent = 0
            while total_sent < len(file_data_bytes):
                sent = sock.send(file_data_bytes[total_sent:total_sent + chunk_size])
                if sent == 0:
                    raise RuntimeError("Socket connection broken")
                total_sent += sent

        # Terima header JSON dari server sampai \r\n\r\n
        header_json_str, rest_data = recv_until(sock)

        response = json.loads(header_json_str.strip())

        # Jika response OK dan ada filesize, baca sisa file data dari socket
        if response.get('status') == 'OK':
            data = response.get('data')
            if isinstance(data, dict) and 'filesize' in data:
                filesize = data['filesize']
                file_data = rest_data
                # Baca sisa file jika belum lengkap
                while len(file_data) < filesize:
                    more_data = sock.recv(524288)
                    if not more_data:
                        break
                    file_data += more_data
                # Simpan file_data ke response supaya fungsi pemanggil bisa akses
                response['file_data_bytes'] = file_data

        return response

    except Exception as e:
        logging.error(f"send_command error: {e}")
        return {"status": "ERROR", "data": str(e)}
    finally:
        sock.close()

def remote_upload(server_ip, server_port, filepath=""):
    try:
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)

        # Kirim perintah upload dengan metadata file_size
        command_str = f"UPLOAD {filename} {file_size}"

        with open(filepath, 'rb') as f:
            file_data_bytes = f.read()

        return send_command(server_ip, server_port, command_str, file_data_bytes)

    except Exception as e:
        logging.error(f"remote_upload error: {e}")
        return {"status": "ERROR", "data": str(e)}

def remote_download(server_ip, server_port, filename=""):
    command_str = f"GET {filename} 0"
    result = send_command(server_ip, server_port, command_str)
    #logging.info(f"Response from server: {result}")

    if result.get('status') == 'OK':
        try:
            data = result.get('data')
            if not data or 'filename' not in data or 'filesize' not in data:
                raise ValueError("Missing filename or filesize in server response data")

            filename = data['filename']
            filesize = data['filesize']

            file_data_bytes = result.get('file_data_bytes')
            if not file_data_bytes or len(file_data_bytes) < filesize:
                raise ValueError("File data incomplete or missing")

            save_path = f"download_{filename}"
            with open(save_path, 'wb') as fp:
                fp.write(file_data_bytes)

            result['download_path'] = save_path
        except Exception as e:
            logging.error(f"remote_download error writing file: {e}")
            result = {"status": "ERROR", "data": str(e)}
    else:
        logging.error(f"Download failed, status: {result.get('status')}, message: {result.get('data')}")
    return result

def worker_task(server_ip, server_port, operation, filepath):
    start_time = time.time()
    if operation == "upload":
        result = remote_upload(server_ip, server_port, filepath)
        byte_size = os.path.getsize(filepath) if result.get('status') == 'OK' else 0
    elif operation == "download":
        result = remote_download(server_ip, server_port, filepath)
        try:
            download_path = result.get('download_path')
            if result.get('status') == 'OK' and download_path and os.path.exists(download_path):
                byte_size = os.path.getsize(download_path)
            else:
                byte_size = 0
        except Exception as e:
            logging.error(f"Error getting size of downloaded file: {e}")
            byte_size = 0
    else:
        result = {"status": "ERROR", "data": "Unknown operation"}
        byte_size = 0
    duration = time.time() - start_time
    return (result.get('status') == 'OK', duration, byte_size)

def stress_test(server_ip, server_port, operation, file_path, pool_mode, pool_size, server_workers, nomor, output_csv):
    executor_cls = ThreadPoolExecutor if pool_mode == "thread" else ProcessPoolExecutor
    results = []
    start_all = time.time()

    with executor_cls(max_workers=pool_size) as executor:
        futures = [executor.submit(worker_task, server_ip, server_port, operation, file_path) for _ in range(pool_size)]
        for f in futures:
            results.append(f.result())

    total_worker_time = sum(r[1] for r in results)  # sum durasi per worker
    total_bytes = sum(r[2] for r in results)
    success_count = sum(1 for r in results if r[0])
    fail_count = pool_size - success_count

    avg_time_per_client = total_worker_time / pool_size if pool_size > 0 else 0
    throughput_per_client = total_bytes / total_worker_time if total_worker_time > 0 else 0

    file_volume = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    file_volume_str = f"{round(file_volume / 1024 / 1024)}MB"

    header = [
        "Nomor", "Operasi", "Volume", "Jumlah client worker pool", "Jumlah server worker pool",
        "Waktu total per client (s)", "Throughput per client (bytes/s)",
        "Jumlah worker client yang sukses dan gagal", "Jumlah worker server yang sukses dan gagal"
    ]
    row = [
        nomor, operation, file_volume_str, pool_size, server_workers,
        round(avg_time_per_client, 3), round(throughput_per_client, 3),
        f"{success_count} sukses, {fail_count} gagal",
        f"{server_workers} server worker, 0 gagal"
    ]
    write_header = not os.path.exists(output_csv)

    with open(output_csv, mode="a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if write_header:
            writer.writerow(header)
        writer.writerow(row)

    print(f"Hasil stress test disimpan ke {output_csv}")
    print("Data:", row)

def main():
    parser = argparse.ArgumentParser(description="File client CLI test and stress test")
    parser.add_argument("--server", default="172.16.16.101", help="Server IP address")
    parser.add_argument("--port", type=int, default=10001, help="Server port")
    parser.add_argument("--mode", choices=["upload", "download", "stress"], required=True, help="Operation mode")
    parser.add_argument("--file", help="File path for upload or filename for download")
    parser.add_argument("--pool_mode", choices=["thread", "process"], default="thread", help="Pool mode for stress test")
    parser.add_argument("--pool_size", type=int, default=1, help="Number of concurrent workers")
    parser.add_argument("--server_workers", type=int, default=1, help="Number of server workers (for logging only)")
    parser.add_argument("--nomor", type=int, default=1, help="Nomor test case untuk laporan")
    parser.add_argument("--output", default="stress_test_report.csv", help="Output CSV file name")
    args = parser.parse_args()

    if args.mode == "upload":
        if not args.file:
            print("Upload mode requires --file argument")
            return
        res = remote_upload(args.server, args.port, args.file)
        print(res)
    elif args.mode == "download":
        if not args.file:
            print("Download mode requires --file argument")
            return
        res = remote_download(args.server, args.port, args.file)
        print(res)
    elif args.mode == "stress":
        if not args.file:
            print("Stress test requires --file argument")
            return
        stress_test(
            args.server, args.port, "upload", args.file,
            args.pool_mode, args.pool_size, args.server_workers,
            args.nomor, args.output
        )

if __name__ == "__main__":
    main()

