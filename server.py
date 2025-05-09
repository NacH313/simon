import threading
import time
from collections import defaultdict


class TupleSpace:
    def __init__(self):
        self._tuples = {}
        self._lock = threading.Lock()
        self._stats = {
            'total_operations': 0,
            'total_reads': 0,
            'total_gets': 0,
            'total_puts': 0,
            'total_errors': 0
        }
        self._client_count = 0
        self._last_report_time = time.time()

    def _calculate_stats(self):
        with self._lock:
            tuple_count = len(self._tuples)
            total_size = sum(len(k) + len(v) for k, v in self._tuples.items())
            avg_size = total_size / tuple_count if tuple_count > 0 else 0

            key_sizes = sum(len(k) for k in self._tuples.keys())
            avg_key_size = key_sizes / tuple_count if tuple_count > 0 else 0

            value_sizes = sum(len(v) for v in self._tuples.values())
            avg_value_size = value_sizes / tuple_count if tuple_count > 0 else 0

            return {
                'tuple_count': tuple_count,
                'avg_size': avg_size,
                'avg_key_size': avg_key_size,
                'avg_value_size': avg_value_size,
                'total_clients': self._client_count,
                **self._stats
            }

    def _generate_report(self):
        stats = self._calculate_stats()
        report = (
            f"Tuple Space Report:\n"
            f"- Tuples: {stats['tuple_count']}\n"
            f"- Avg Tuple Size: {stats['avg_size']:.2f}\n"
            f"- Avg Key Size: {stats['avg_key_size']:.2f}\n"
            f"- Avg Value Size: {stats['avg_value_size']:.2f}\n"
            f"- Total Clients: {stats['total_clients']}\n"
            f"- Total Operations: {stats['total_operations']}\n"
            f"- READs: {stats['total_reads']}\n"
            f"- GETs: {stats['total_gets']}\n"
            f"- PUTs: {stats['total_puts']}\n"
            f"- Errors: {stats['total_errors']}\n"
        )
        return report

    def increment_client_count(self):
        with self._lock:
            self._client_count += 1

    def decrement_client_count(self):
        with self._lock:
            self._client_count -= 1

    def periodic_report(self, interval=10):
        while True:
            time.sleep(interval)
            current_time = time.time()
            if current_time - self._last_report_time >= interval:
                report = self._generate_report()
                print(report)
                self._last_report_time = current_time

    def process_request(self, command, key, value=None):
        with self._lock:
            self._stats['total_operations'] += 1

            if command == 'R':  # READ
                self._stats['total_reads'] += 1
                if key in self._tuples:
                    return f"OK({key}, {self._tuples[key]}) read"
                else:
                    self._stats['total_errors'] += 1
                    return f"ERR {key} does not exist"

            elif command == 'G':  # GET
                self._stats['total_gets'] += 1
                if key in self._tuples:
                    value = self._tuples.pop(key)
                    return f"OK({key}, {value}) removed"
                else:
                    self._stats['total_errors'] += 1
                    return f"ERR {key} does not exist"

            elif command == 'P':  # PUT
                self._stats['total_puts'] += 1
                if key in self._tuples:
                    self._stats['total_errors'] += 1
                    return f"ERR {key} already exists"
                else:
                    self._tuples[key] = value
                    return f"OK({key}, {value}) added"

            else:
                self._stats['total_errors'] += 1
                return "ERR invalid command"
            import socket
            import threading
            from .tuple_space import TupleSpace

            class ClientHandler(threading.Thread):
                def __init__(self, client_socket, client_address, tuple_space):
                    threading.Thread.__init__(self)
                    self.client_socket = client_socket
                    self.client_address = client_address
                    self.tuple_space = tuple_space

                def run(self):
                    try:
                        self.tuple_space.increment_client_count()
                        with self.client_socket:
                            while True:
                                data = self.client_socket.recv(1024)
                                if not data:
                                    break

                                # Parse the request
                                try:
                                    size_str = data[:3]
                                    if not size_str.isdigit():
                                        raise ValueError("Invalid size prefix")

                                    size = int(size_str)
                                    if size != len(data):
                                        raise ValueError("Size mismatch")

                                    command = data[3]
                                    key_value = data[4:].decode('utf-8')

                                    if command in ['R', 'G']:
                                        # READ or GET - only key is provided
                                        if len(key_value.split(' ', 1)) != 1:
                                            raise ValueError("Invalid format for READ/GET")
                                        key = key_value
                                        response = self.tuple_space.process_request(command, key)
                                    elif command == 'P':
                                        # PUT - key and value are provided
                                        parts = key_value.split(' ', 1)
                                        if len(parts) != 2:
                                            raise ValueError("Invalid format for PUT")
                                        key, value = parts
                                        response = self.tuple_space.process_request(command, key, value)
                                    else:
                                        raise ValueError("Invalid command")

                                    # Send response
                                    response_data = f"{len(response):03d}{response}".encode('utf-8')
                                    self.client_socket.sendall(response_data)

                                except Exception as e:
                                    error_response = f"ERR {str(e)}"
                                    response_data = f"{len(error_response):03d}{error_response}".encode('utf-8')
                                    self.client_socket.sendall(response_data)
                                    break

                    finally:
                        self.tuple_space.decrement_client_count()
                        import socket
                        import threading
                        from .tuple_space import TupleSpace
                        from .client_handler import ClientHandler

                        class Server:
                            def __init__(self, host='0.0.0.0', port=51234):
                                self.host = host
                                self.port = port
                                self.tuple_space = TupleSpace()
                                self.server_socket = None

                            def start(self):
                                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                self.server_socket.bind((self.host, self.port))
                                self.server_socket.listen(5)
                                print(f"Server started on {self.host}:{self.port}")

                                # Start periodic reporting thread
                                report_thread = threading.Thread(target=self.tuple_space.periodic_report, daemon=True)
                                report_thread.start()

                                try:
                                    while True:
                                        client_socket, client_address = self.server_socket.accept()
                                        print(f"Accepted connection from {client_address}")
                                        handler = ClientHandler(client_socket, client_address, self.tuple_space)
                                        handler.start()
                                except KeyboardInterrupt:
                                    print("Server shutting down...")
                                finally:
                                    if self.server_socket:
                                        self.server_socket.close()

                        if __name__ == "__main__":
                            import sys
                            if len(sys.argv) > 1:
                                port = int(sys.argv[1])
                            else:
                                port = 51234

                            server = Server(port=port)
                            server.start()
                            import re

                            class RequestParser:
                                @staticmethod
                                def parse_line(line):
                                    line = line.strip()
                                    if not line:
                                        return None

                                    # Check for PUT command
                                    put_match = re.match(r'^PUT\s+(\S+)\s+(.*)$', line)
                                    if put_match:
                                        key, value = put_match.groups()
                                        if len(key) + len(value) + len("PUT ") > 970:
                                            return None
                                        return ('P', key, value)

                                    # Check for READ or GET command
                                    cmd_match = re.match(r'^(READ|GET)\s+(\S+)$', line)
                                    if cmd_match:
                                        cmd, key = cmd_match.groups()
                                        command = 'R' if cmd == 'READ' else 'G'
                                        if len(key) + len(cmd) + 1 > 970:  # +1 for space
                                            return None
                                        return (command, key, None)

                                    return None