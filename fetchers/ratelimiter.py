import logging
import time


class RateLimiter:
    def __init__(self, sleep_sec: float):
        self.sleep_sec = sleep_sec
        self.logger = logging.getLogger(__name__)

    def sleep(self):
        if self.sleep_sec and self.sleep_sec > 0:
            time.sleep(self.sleep_sec)
            self.logger.info("Invoking sleep.")
