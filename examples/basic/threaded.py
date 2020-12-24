# pip install requests

import uuid
import threading
import signal
import time
import random

import dbi
from dinao.backend import create_connection_pool


class Smacker(threading.Thread):

    def __init__(self, worker_num: int):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.worker_num = worker_num

    def _db_action(self):
        pass

    def run(self):
        tid = threading.get_ident()
        tid = f"{tid}-{self.__class__.__name__}"
        cnt = 0
        print(f"{tid}: Starting")
        while not self.shutdown_flag.is_set():
            cnt += 1
            self._db_action()
            if cnt % 100 == 0:
                print(f"{tid}: Made {cnt} calls")
        print(f"{tid}: Shutting down")


class SmackInsert(Smacker):

    def _db_action(self):
        name = f"testing_{self.worker_num}_{str(uuid.uuid4())}"
        value = random.randint(1, 100)
        dbi.upsert(name, value)


class SmackSearch(Smacker):

    def _db_action(self):
        search = f"testing_{self.worker_num}_%"
        page = {
            "limit": random.randint(1, 200),
            "offset": random.randint(0, 10),
        }
        dbi.search(search, page)


class SmackerSummer(Smacker):

    def _db_action(self):
        search = f"testing_{self.worker_num}_%"
        page_size = random.randint(1, 200)
        dbi.sum_for(search, page_size)


class ShutdownNow(Exception):

    @classmethod
    def raise_it(cls, signal_code, frame):
        raise cls


def main():
    signal.signal(signal.SIGTERM, ShutdownNow.raise_it)
    signal.signal(signal.SIGINT, ShutdownNow.raise_it)
    # We make 30 workers, so we need a thread safe pool with 30 connections
    con_url = "postgresql://test_user:test_pass@localhost:5432/test_db?pool_threaded=True&pool_max_conn=30"
    db_pool = create_connection_pool(con_url)
    dbi.binder.pool = db_pool
    dbi.make_table()
    workers = 10
    smackers = []
    try:
        smackers = [SmackInsert(w) for w in range(workers)]
        smackers += [SmackSearch(w) for w in range(workers)]
        smackers += [SmackerSummer(w) for w in range(workers)]
        for smacker in smackers:
            smacker.start()
        while True:
            time.sleep(0.5)
    except ShutdownNow:
        for smacker in smackers:
            smacker.shutdown_flag.set()
            smacker.join()


if __name__ == "__main__":
    main()
