import configparser
import logging
import os
import time
from functools import wraps
import smtplib, ssl


class CustomLogFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""
    grey = "\x1b[38;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


logging.basicConfig(filename="output/crawl.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger("Crawler")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomLogFormatter())
logger.addHandler(ch)
# Disable propagating urllib3 logs
logging.getLogger("urllib3").setLevel(logging.ERROR)


def timeit(function):
    """
    Decorator function to track the time a method takes to run
    @param function: input function
    @return: result of input function
    """

    @wraps(function)
    def timer(*args, **kwargs):
        start = time.time()
        result = function(*args, **kwargs)
        end = time.time()
        m, s = divmod(int(end - start), 60)
        logger.info(f"{function.__qualname__} took {m:02d}:{s:02d} m/s")
        return result

    return timer


def batch(iterable, n=1):
    """
    Returns batches of data for iterating purposes
    @param iterable: input data to  iterate over
    @param n: batch-size
    @return: iterable which yields batch-sized outputs
    """
    it_length = len(iterable)
    for ndx in range(0, it_length, n):
        yield iterable[ndx:min(ndx + n, it_length)]


config = configparser.ConfigParser()
config.read("config.ini")

if "TWITTER_CRAWLER_SENDER" in os.environ:
    sender = os.environ.get("TWITTER_CRAWLER_SENDER")
else:
    sender = config["mail"]["Sender"]
if "TWITTER_CRAWLER_SENDER_PW" in os.environ:
    password = os.environ.get("TWITTER_CRAWLER_SENDER_PW")
else:
    password = config["mail"]["Password"]
if "TWITTER_CRAWLER_RECEIVER" in os.environ:
    receiver = os.environ.get("TWITTER_CRAWLER_RECEIVER")
else:
    receiver = config["mail"]["Receiver"]


def send_warn_mail():
    """
    Sends warning mail to defined receiver stating that the monthly usage cap is exceeded
    """
    txt = "Twitter crawler exceeded monthly usage cap"
    message = 'Subject: {}\n\n{}'.format("Monthly Usage Cap Exceeded", txt)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(config["mail"]["SMTP"], config["mail"]["Port"], context=context) as server:
        server.login(sender, password)
        server.sendmail(sender, receiver, message)
