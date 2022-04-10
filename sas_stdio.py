import logging
import os
import re
import select

STDOUT_FILENO, STDERR_FILENO = 1, 2

from io_helper import DataReader, LineReader, Writer


class SASsession:
    def __init__(self, sascmd):
        self.timeout = 0.1

        self._sas = self._startsas(sascmd)

        self._inp = Writer(self._sas["stdin"])
        self._out = DataReader(self._sas["stdout"])
        self._err = LineReader(self._sas["stderr"],
            handler=self._process_log_line, partial_handler=self._process_log_line
        )

        pattern = b"0e831b8c59cb88d1"

        self._finish_line = pattern
        self._put_finish = b"%%put %s;" % self._finish_line
        self._re_put_finish = re.compile(b"\d+ *" + self._put_finish)

        self._re_cancel_line = re.compile(b"ERROR: .* %?ABORT CANCEL.*")
        self._force_cancel = b"%%macro __%s; %%abort cancel; %%mend; %%__%s" % (pattern, pattern)

        self.initial_results = self.submit(b"")

    def submit(self, code):
        code += b"\n" + self._put_finish + b"\n"

        self._end = None
        self._canceled = False

        self._inp.write(code)

        self._out.clear()
        self._err.clear()

        rfds, timeout = [self._out.fd, self._err.fd], None
        while select.select(rfds, [], [], timeout)[0]:
            self._out.read()
            self._err.read()

            if not self._sasrunning():
                break
            if self._end is not None:
                timeout = self.timeout

        return {"LST": self._out.data, "LOG": self._err.data(self._end)}

    def _startsas(self, sascmd):
        ro, wo = os.pipe()  # stdout
        re, we = os.pipe()  # stderr

        pid, master = os.forkpty()

        if pid == 0:  # child
            for fd in [ro, re]:
                os.close(fd)
            for fd_new, fd_old in [(STDOUT_FILENO, wo), (STDERR_FILENO, we)]:
                os.close(fd_new)
                os.dup2(fd_old, fd_new)
                os.close(fd_old)
            os.execv(sascmd[0], sascmd)

        for fd in [wo, we]:
            os.close(fd)

        return {
            "pid": pid,
            "stdin": os.fdopen(master, mode="wb"),
            "stdout": os.fdopen(ro, mode="rb"),
            "stderr": os.fdopen(re, mode="rb"),
        }

    def _sasrunning(self):
        pid = self._sas["pid"]
        if pid is None or os.waitpid(pid, os.WNOHANG)[0] != 0:
            logging.info("SAS not running")
            self._sas["pid"] = None
            return False
        return True

    def _process_log_line(self, line, line_number):
        if self._end is None:
            if self._re_put_finish.match(line) or line == self._finish_line:
                logging.info("submit finished")
                self._end = line_number
            elif not self._canceled and self._re_cancel_line.match(line):
                logging.info("cancelation suspected")
                self._force_cancelation()

    def _force_cancelation(self):
        logging.info(f"force cancelation")
        self._inp.write(self._force_cancel + b"\n\x04" + self._put_finish + b"\n")
        self._canceled = True
