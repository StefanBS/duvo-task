import signal
import threading


def stop_event() -> threading.Event:
    """An Event set on SIGTERM/SIGINT, so loops can finish in-flight work and exit."""
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    return stop
