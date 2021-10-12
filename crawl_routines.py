import datetime
import json
import time
import simplejson.errors
import mongo_db as db
from test_api import ApiEndpoints
from helper import logger

crawler = ApiEndpoints()


def write_file(response, out_file):
    try:
        response = response["data"]
        out_file.write(json.dumps(response, indent=4, sort_keys=True))
    except simplejson.errors.JSONDecodeError:
        logger.info("Failed to write response to file")


def write_db(response):
    response = response["data"]
    db.insert(response)


def recursive_crawl(crawl_function, params, out_file):
    time.sleep(0.5)
    response = crawl_function(**params)
    remaining = int(response.headers["x-rate-limit-remaining"])
    max_remaining = int(response.headers["x-rate-limit-limit"])
    limit_reset_time = int(response.headers["x-rate-limit-reset"])
    try:
        res_json = response.json()
        write_db(res_json)
    except simplejson.errors.JSONDecodeError:
        logger.info("Rate Limit Error on first request --> wait on limit reset")
        return limit_reset_time
    if "next_token" not in res_json["meta"]:
        logger.info("Successfully crawled ")
        return None
    elif remaining == 0:
        # Next_token available but crawl limit reached
        logger.info(
            "Crawl Limit reached max crawls: {} next reset time: {}".format(max_remaining, limit_reset_time))
        return limit_reset_time
    else:
        # More results available --> use next_token
        next_token = res_json["meta"]["next_token"]
        logger.info("Next crawl --> Pagination token " + next_token)
        params["next_token"] = next_token
        return recursive_crawl(crawl_function, params, out_file)


def crawl(crawl_function, params, out_file):
    next_crawl_time = time.time()
    while next_crawl_time is not None:
        next_crawl_time = recursive_crawl(crawl_function, params, out_file)
        logger.info("Next Crawl Time {}".format(next_crawl_time))
        if next_crawl_time is None:
            # Crawl done without exceeding any limits
            break
        logger.info(
            "Wait until limit reset in " + str(
                datetime.timedelta(seconds=next_crawl_time - int(time.time()))) + " h/m/s")
        try:
            time.sleep(next_crawl_time - time.time())
        except ValueError():
            logger.info("Limit reset done")


# SEED_TWEET_ID = "1433361036191612930" # toni
# SEED_TWEET_ID = "1442243266280370177" #vanderhorst
SEED_TWEET_ID = "1158074774297468928"  # neildegrasstyson

out_file = open("output/crawl_tweets.txt", "w")

# SEED TWEET
logger.info("Crawl seed tweet")
tweet_response = crawler.get_tweets_by_id([SEED_TWEET_ID])
res_json = tweet_response.json()
write_file(res_json, out_file)
write_db(res_json)

# AUTHOR OF TWEET
out_file.write("\nAUTHOR OF TWEET \n")
logger.info("Crawl author")
author_id = res_json["data"][0]["author_id"]
tweet_text = res_json["data"][0]["text"]
user_response = crawler.get_users_by_id([author_id])
res_json = user_response.json()
username = res_json["data"][0]["username"]
write_file(res_json, out_file)
write_db(res_json)

# LIKERS OF TWEET
out_file.write("\nLIKER OF TWEET \n")
logger.info("Crawl liker")
like_response = crawler.get_liking_users(SEED_TWEET_ID)
res_json = like_response.json()
write_file(res_json, out_file)
write_db(res_json)

# # FOLLOWER OF USER
# out_file.write("FOLLOWER \n")
# logger.info("Crawl followers")
# quote_params = {
#     "user_id": author_id,
#     "except_fields": None,
#     "next_token": None
# }
# crawl(crawl_function=crawler.get_followers, params=quote_params, out_file=out_file)
#
# # USER IS FOLLOWING
# f.write("FOLLOWER \n")
# logger.info("Crawl following")
# quote_params = {
#     "user_id": author_id,
#     "except_fields": None,
#     "next_token": None
# }
# crawl.crawl(crawl_function=crawler.get_following, params=quote_params, out_file=out_file)
#

# TIMELINE OF AUTHOR
# out_file.write("QUOTES \n")
# logger.info("Crawl timeline")
# timeline_params = {
#     "user_id": author_id,
#     "except_fields": "default",
#     "next_token": None
# }
# crawl(crawl_function=crawler.get_timeline, params=timeline_params, out_file=out_file)

#
# # ALL RETWEETER BY TEXT ARCHIVE SEARCH
# f.write("RETWEETER \n")
# logger.info("Crawl retweet")
# try:
#     author_name = res_json["data"][0]["username"]
#     retweet_response = crawl.crawler.get_retweets_archive_search(author_name, tweet_text)
#     res_json = retweet_response.json()
#     crawl.write_db(res_json, f)
# except KeyError:
#     logger.info("No retweets found using full-archive search")
#
# # 100 MOST RECENT RETWEETER
# f.write("RETWEETER2 \n")
# logger.info("Crawl 100 most recent retweets")
# retweet_response = crawl.crawler.get_retweeting_users(SEED_TWEET_ID)
# res_json = retweet_response.json()
# crawl.write_db(res_json, f)
#
# # # QUOTES OF TWEET
# f.write("QUOTES \n")
# logger.info("Crawl quotes")
# quote_params = {
#     "username": username,
#     "tweet_id": SEED_TWEET_ID,
#     "except_fields": None,
#     "next_token": None
# }
# crawl.crawl(crawl_function=api.get_quotes, params=quote_params, out_file=f)
#
# CONVERSATION (REPLY) TREE
# f.write("REPLIES \n")
# logger.info("Crawl replies")
# reply_params = {
#     "tweet_id": SEED_TWEET_ID,
#     "except_fields": None,
#     "next_token": None
# }
# crawl.crawl(crawl_function=api.get_replies, params=reply_params, out_file=f)
