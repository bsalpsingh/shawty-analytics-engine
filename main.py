import signal
import sys
from app.core.subscriber import SubscriberSingleton


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    SubscriberSingleton().consume()


if __name__ == "__main__":
    main()