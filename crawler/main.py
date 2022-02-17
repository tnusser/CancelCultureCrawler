import datetime
import queue
from threading import Thread
import crawl_routines as bot
from crawl_routines import EventSearch
from utils import *

event_list = [
    #EventSearch(uid=-1, tweet_id="1442243266280370177", start_date=None, days=14, tag_and_mention=None, username=None,
    #            comment="vanderhorst"),
    #EventSearch(uid=0, tweet_id="1433361036191612930", start_date=None, days=14, tag_and_mention=None, username=None,
    #            comment="toni test"),
    #EventSearch(uid=1, tweet_id="1158074774297468928", start_date="2019-08-04T18:00:00.000Z", days=14,
    #            tag_and_mention={"#neildegrassetyson"},
    #            username="neiltyson", comment="neil de grasse tyson"),
    # EventSearch(uid=8, tweet_id="1327806477923323904", start_date="2020-11-15T14:00:00.000Z", days=14,
    #             tag_and_mention=None,
    #             username="ginacarano", comment="gina carano"),
    # EventSearch(uid=2, tweet_id="1265998625836019712", start_date="2020-06-28T14:00:00.000Z", days=14,
    #             tag_and_mention=None,
    #             username="davidshor", comment="david shor"),
    # EventSearch(uid=3, tweet_id="1269382518362509313", start_date="2020-06-06T23:00:00.000Z", days=14,
    #             tag_and_mention=None,
    #             username="jk_rowling", comment="j.k. rowling 1"),
    # EventSearch(uid=4, tweet_id="1269389298664701952", start_date="2020-06-07T12:00:00.000Z", days=14,
    #             tag_and_mention=None,
    #             username="jk_rowling", comment="j.k. rowling 2"),
    EventSearch(uid=9, tweet_id="1327806477923323904", start_date="2020-06-09T03:00:00.000Z", days=14,
                tag_and_mention={"@haralduhlig"}, username="haralduhlig", comment="harald uhlig")
    #EventSearch(uid=5, tweet_id=None, start_date="2018-05-17T23:00:00.000Z", days=14,
    #            tag_and_mention={"#AaronMSchlossberg", "#AaronSchlossberg"},
    #            username=None, comment="aaron schlossberg"),
    #EventSearch(uid=6, tweet_id=None, start_date="2018-05-08T23:00:00.000Z", days=14, tag_and_mention={"#KellyPocha"},
    #            username=None, comment="kelly pocha"),
    #EventSearch(uid=7, tweet_id="1324385598539399168", start_date="2020-11-05T14:00:00.000Z", days=14,
    #            tag_and_mention={"#FireGinaCarano"},
    #            username="ginacarano", comment="gina carano"),
]

if __name__ == "__main__":
    # TODO Remove following line if you want to iterate through the whole event list
    event_list = []
    for event in event_list:
        logger.info(f"Start crawl of {event}")
        if event.tweet_id is not None:
            bot.seed_tweet_id = event.tweet_id
            bot.get_seed(event.tweet_id)
            bot.pipeline(event.tweet_id)
        if event.tag_and_mention is not None:
            start = datetime.datetime.strptime(event.start_date, '%Y-%m-%dT%H:%M:%S.%fZ')
            end_date = start + datetime.timedelta(days=event.days)
            bot.hashtag_or_mention(event.tag_and_mention, start=event.start_date, end=end_date.isoformat("T")+"Z")
    logger.info("Successfully crawled conversation trees")
    crawl_queue = queue.Queue()
    crawl_queue.put(bot.crawl_likes)
    crawl_queue.put(bot.crawl_retweets)
    crawl_queue.put(bot.crawl_timelines)
    crawl_queue.put(bot.crawl_following)
    crawl_queue.put(bot.crawl_follows)

    for j in range(crawl_queue.qsize()):
        logger.info(f"Main: create and start thread for crawl queue {j}")
        Thread(target=bot.crawl_worker, args=(crawl_queue,), daemon=True).start()
    crawl_queue.join()
