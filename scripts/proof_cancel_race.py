"""macOS cancel race (pi #11252 / 27cc6bfc0): interrupting `sleep 30 & sleep 30; echo NEVER`
through hermes' real LocalEnvironment.execute must never let `echo NEVER` run.

Each trial: a worker thread runs execute() on a command that writes a marker file when its
second step starts; after ~200 ms the main thread interrupts that worker (tools.interrupt, the
/stop path). PARALLEL copies run at once while CPU burners load the machine. Prints one RESULT
line: {"trials": N, "leaked": K}.
"""
import json
import multiprocessing
import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.environ.get("TREE", os.getcwd()))
from tools import interrupt  # noqa: E402
from tools.environments.local import LocalEnvironment  # noqa: E402

TRIALS = int(os.environ.get("TRIALS", "150"))
PARALLEL = int(os.environ.get("PARALLEL", "6"))


def burn(stop):
    while not stop.is_set():
        sum(i * i for i in range(20000))


def trial(work):
    marker = os.path.join(work, f"m{os.getpid()}-{threading.get_ident()}-{time.monotonic_ns()}")
    env = LocalEnvironment(cwd=work, timeout=60)
    tid = []
    done = threading.Event()

    def run():
        tid.append(threading.get_ident())
        try:
            env.execute(f"sleep 30 & sleep 30; echo NEVER > {marker}", timeout=60)
        finally:
            done.set()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    while not tid:
        time.sleep(0.005)
    time.sleep(0.2)
    interrupt.set_interrupt(True, tid[0])
    done.wait(30)
    interrupt.set_interrupt(False, tid[0])
    time.sleep(0.3)  # give a leaked `echo` time to land
    leaked = os.path.exists(marker)
    with __import__("contextlib").suppress(OSError):
        os.remove(marker)
    return leaked


if __name__ == "__main__":
    stop = multiprocessing.Event()
    burners = [multiprocessing.Process(target=burn, args=(stop,), daemon=True)
               for _ in range(max(2, (os.cpu_count() or 4)))]
    for b in burners:
        b.start()
    work = tempfile.mkdtemp(prefix="cancel-race-")
    try:
        with ThreadPoolExecutor(PARALLEL) as pool:
            results = list(pool.map(lambda _i: trial(work), range(TRIALS)))
    finally:
        stop.set()
        for b in burners:
            b.join(5)
    print("RESULT " + json.dumps({"trials": TRIALS, "leaked": sum(results),
                                  "platform": sys.platform, "cpus": os.cpu_count()}))
