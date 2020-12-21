# pip install requests

import uuid
import threading
import signal
import time
import random

import requests


class Smacker(threading.Thread):

    def __init__(self, worker_num: int):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.api = "http://localhost:5000/"
        self.worker_num = worker_num

    def _api_action(self) -> requests.Response:
        pass

    def run(self):
        tid = threading.get_ident()
        cnt = 0
        print(f"{tid}: Starting")
        while not self.shutdown_flag.is_set():
            cnt += 1
            res = self._api_action()
            if res.status_code != 200:
                print(f"{tid}: Got non 200: {res.status_code} {res.text}")
            if cnt % 100 == 0:
                print(f"{tid}: Made {cnt} calls")
        print(f"{tid}: Shutting down")


class SmackPutter(Smacker):

    def _api_action(self):
        name = f"testing_{self.worker_num}_{str(uuid.uuid4())}"
        value = random.randint(1, 100)
        return requests.post(self.api, json={'name': name, 'value': value})


class SmackerLister(Smacker):

    def _api_action(self):
        params = {
            "page": random.randint(1, 5),
            "size": random.randint(1, 200),
            "search": f"testing_{self.worker_num}_%",
        }
        return requests.get(self.api, params=params)


class SmackerSummer(Smacker):

    def _api_action(self):
        params = {
            "search": f"testing_{self.worker_num}%",
            "size": random.randint(1, 200),
        }
        return requests.get(f"{self.api}/summed", params=params)


class ShutdownNow(Exception):

    @classmethod
    def raise_it(cls, signal_code, frame):
        raise cls


def main():
    signal.signal(signal.SIGTERM, ShutdownNow.raise_it)
    signal.signal(signal.SIGINT, ShutdownNow.raise_it)
    workers = 10
    smackers = []
    try:
        smackers = [SmackerLister(w) for w in range(workers)]
        smackers += [SmackPutter(w) for w in range(workers)]
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
