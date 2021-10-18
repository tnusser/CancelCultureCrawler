import datetime
import json
import simplejson.errors
import mongo_db as db
from test_api import ApiEndpoints
from helper import *

api = ApiEndpoints()


def write_file(response, out_file):
    try:
        # response = response["data"]
        out_file.write(json.dumps(response, indent=4, sort_keys=True))
    except simplejson.errors.JSONDecodeError:
        logger.info("Failed to write response to file")


def write_db(response, collection="cc_tweets", cache=False):
    response = response["data"]
    if cache:
        for res in response:
            if res["id"] not in tweet_cache:
                tweet_cache[res["id"]] = res["public_metrics"]
            if res["author_id"] not in author_cache:
                author_cache[res["author_id"]] = {}
    db.insert(response, collection)


def recursive_crawl(crawl_function, params, collection, cache):
    # TODO Figure out min time.sleep needed
    time.sleep(0.4)
    response = crawl_function(**params)
    remaining = int(response.headers["x-rate-limit-remaining"])
    max_remaining = int(response.headers["x-rate-limit-limit"])
    limit_reset_time = int(response.headers["x-rate-limit-reset"])
    try:
        response_json = response.json()
        write_file(response_json, out_file)
        write_db(response_json, collection, cache)
    except simplejson.errors.JSONDecodeError:
        logger.info("Rate Limit Error on first request --> wait on limit reset")
        return limit_reset_time
    if "next_token" not in response_json["meta"]:
        logger.info("Successfully crawled ")
        return None
    elif remaining == 0:
        # Next_token available but crawl limit reached
        logger.info(
            "Crawl Limit reached max crawls: {} next reset time: {}".format(max_remaining, limit_reset_time))
        return limit_reset_time
    else:
        # More results available --> use next_token
        next_token = response_json["meta"]["next_token"]
        logger.info("Next crawl --> Pagination token " + next_token)
        params["next_token"] = next_token
        return recursive_crawl(crawl_function, params, collection, cache)


def crawl(crawl_function, params, collection="cc_tweets", cache=False):
    next_crawl_time = time.time()
    while next_crawl_time is not None:
        next_crawl_time = recursive_crawl(crawl_function, params, collection, cache)
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


# TODO Add crawling time and cc_event_id/name to db
# TODO Add ids?
# TODO cache user and timeline (id) crawls here

events = {"1433361036191612930": 0,  # toni test tweet
          "1158074774297468928": 1,  # neil degrasse tyson
          } # todo add ids to db

SEED_TWEET_ID = "1433361036191612930"  # toni
# SEED_TWEET_ID = "1442243266280370177" #vanderhorst
# SEED_TWEET_ID = "1158074774297468928"  # neildegrasstyson

out_file = open("output/crawl_tweets.txt", "w")
tweet_cache, author_cache = {}, {}


@timeit
def seed_tweet(tweet_id):
    logger.info("Retrieving seed tweet")
    response = api.get_tweets_by_id([tweet_id]).json()
    write_file(response, out_file)
    write_db(response, cache=True)
    return response


@timeit
def reply_tree(tweet_id):
    logger.info("Retrieving replies to seed tweet")
    reply_params = {
        "tweet_id": tweet_id,
        "except_fields": None,
        "next_token": None
    }
    crawl(crawl_function=api.get_replies, params=reply_params, cache=True)


@timeit
def get_user():
    logger.info("Retrieving user information")
    author_ids = list(author_cache.keys())
    for author_id_batch in batch(author_ids, 100):
        user_response = api.get_users_by_id(author_id_batch).json()
        write_file(user_response, out_file)
        write_db(user_response, "cc_users")


@timeit
def quotes():
    logger.info("Retrieve all quotes")
    for tweet_id, stats in tweet_cache.items():
        if stats["quote_count"] > 0:

            print(stats["quote_count"])
            quote_params = {
                "username": username, # todo
                "tweet_id": tweet_id,
                "except_fields": None,
                "next_token": None
            }
            for id in tweet_cache:
                pass
            crawl(crawl_function=api.get_quotes, params=quote_params)


#
# # LIKERS OF TWEET
# out_file.write("\nLIKER OF TWEET \n")
# logger.info("Crawl liker")
# like_response = api.get_liking_users(SEED_TWEET_ID)
# res_json = like_response.json()
# write_file(res_json, out_file)
# write_db(res_json)
#
# # FOLLOWER OF USER
# out_file.write("FOLLOWER \n")
# logger.info("Crawl followers")
# quote_params = {
#     "user_id": author_id,
#     "except_fields": None,
#     "next_token": None
# }
# crawl(crawl_function=api.get_followers, params=quote_params)
#
# # USER IS FOLLOWING
# out_file.write("FOLLOWER \n")
# logger.info("Crawl following")
# quote_params = {
#     "user_id": author_id,
#     "except_fields": None,
#     "next_token": None
# }
# crawl(crawl_function=api.get_following, params=quote_params)
#
#
# # TIMELINE OF AUTHOR
# out_file.write("TIMELINE \n")
# logger.info("Crawl timeline")
# timeline_params = {
#     "user_id": author_id,
#     "except_fields": "default",
#     "next_token": None
# }
# crawl(crawl_function=api.get_timeline, params=timeline_params)
#
#
# # ALL RETWEETER BY TEXT ARCHIVE SEARCH
# out_file.write("RETWEETER \n")
# logger.info("Crawl retweet")
# try:
#     author_name = res_json["data"][0]["username"]
#     retweet_response = api.get_retweets_archive_search(author_name, tweet_text)
#     res_json = retweet_response.json()
#     write_file(res_json, out_file)
#     write_db(res_json)
# except KeyError:
#     logger.info("No retweets found using full-archive search")
#
# # 100 MOST RECENT RETWEETER
# out_file.write("RETWEETER2 \n")
# logger.info("Crawl 100 most recent retweets")
# retweet_response = api.get_retweeting_users(SEED_TWEET_ID)
# res_json = retweet_response.json()
# write_file(res_json, out_file)
# write_db(res_json)
#


if __name__ == "__main__":
    seed_tweet(SEED_TWEET_ID)
    reply_tree(SEED_TWEET_ID)
    get_user()
    quotes() #todo recursive
