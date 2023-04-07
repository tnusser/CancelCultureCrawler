import queue
from threading import Thread
import crawl_routines as bot
from api_endpoints import ApiEndpoints
from utils import *


api = ApiEndpoints()
if __name__ == "__main__":
    crawl_queue = queue.Queue()
    crawl_queue.put(bot.crawl_keyword)
    for j in range(crawl_queue.qsize()):
        logger.info(f"Main: create and start thread for crawl queue {j}")
    Thread(target=bot.crawl_worker, args=(crawl_queue,), daemon=True).start()
    crawl_queue.join()
