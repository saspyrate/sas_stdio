import fcntl
import logging
import os
import queue
import threading

from abc import ABC, abstractmethod


class Writer:
    def __init__(self, fd):
        self.fd = fd
        self._queue = queue.Queue()
        threading.Thread(target=self._writer, daemon=True).start()

    def write(self, data):
        logging.debug(f"write {data}")
        self._queue.put(data)

    def _writer(self):
        while True:
            self.fd.write(self._queue.get())
            self.fd.flush()
            self._queue.task_done()


class Reader(ABC):
    def __init__(self, fd):
        self._init_fd(fd)
        self.clear()

    def _init_fd(self, fd):
        self.fd = fd
        fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK)

    def clear(self):
        pass

    def read(self):
        data = self.fd.read()
        if data is not None:
            self._handle(data)

    @abstractmethod
    def _handle(self, data):
        pass


class DataReader(Reader):
    def clear(self):
        self.data = b""

    def _handle(self, data):
        self.data += data


class LineReader(Reader):
    def __init__(self, fd, handler=None, partial_handler=None, delim=b"\n"):
        super().__init__(fd)
        self.handler = handler
        self.partial_handler = partial_handler
        self.delim = delim

    def data(self, end=None):
        data = b"\n".join(self.lines[:end])
        if self.partial_line is not None and (end is None or end > len(self.lines)):
            data += b"\n" + self.partial_line
        return data

    def clear(self):
        self.partial_line = None
        self.lines = []

    def _handle(self, data):
        logging.debug(f"read {len(data)} bytes {data[:100] + b'...'}")
        complete_data, delim, partial_line = data.rpartition(self.delim)
        lines = complete_data.split(self.delim)
        if self.partial_line is not None:
            lines[0] = self.partial_line + lines[0]
        self.partial_line = partial_line if delim else None
        if self.handler:
            for line in lines:
                logging.debug(f"handle log line {line}")
                self.handler(line, len(self.lines))
                self.lines.append(line)
        if delim and self.partial_handler:
            logging.debug(f"handle partial log line {partial_line}")
            self.partial_handler(partial_line, len(self.lines))
