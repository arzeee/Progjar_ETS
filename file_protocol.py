import json
import logging

from file_interface import FileInterface


class FileProtocol:
    def __init__(self):
        self.file = FileInterface()

    def proses_string(self, string_datamasuk=''):
        logging.warning(f"string diproses: {string_datamasuk}")
        try:
            parts = string_datamasuk.strip().split(" ", 2)
            c_request = parts[0].lower().strip()
            logging.warning(f"memproses request: {c_request}")
            
            if c_request == 'upload' and len(parts) == 3:
                params = [parts[1], parts[2]]
            elif c_request in ['get', 'delete'] and len(parts) == 2:
                params = [parts[1]]
            elif c_request == 'list':
                params = []
            else:
                return json.dumps(dict(status='ERROR', data='invalid command format'))

            cl = getattr(self.file, c_request)(params)
            return json.dumps(cl)
        except Exception as e:
            logging.error(f"Error processing command: {e}")
            return json.dumps(dict(status='ERROR', data='request tidak dikenali'))


if __name__ == '__main__':
    fp = FileProtocol()
    print(fp.proses_string("UPLOAD upload_test.txt dGVzdCBjb250ZW50Cg=="))
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET upload_test.txt"))
    print(fp.proses_string("DELETE upload_test.txt"))
    print(fp.proses_string("LIST"))

