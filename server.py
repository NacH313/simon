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