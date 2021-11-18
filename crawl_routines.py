import json
from datetime import datetime, timedelta
import simplejson.errors
import mongo_db as db
from test_api import ApiEndpoints
from helper import *

api = ApiEndpoints()


class Tweet:
    def __init__(self, tweet_id, public_metrics):
        self.id = tweet_id
        self.reply_count = public_metrics["reply_count"]
        self.retweet_count = public_metrics["retweet_count"]
        self.like_count = public_metrics["like_count"]
        self.quote_count = public_metrics["quote_count"]
        self.quotes_retrieved = False
        self.likes_retrieved = False
        self.retweets_retrieved = False

    def __repr__(self):
        return f"Tweet-ID {self.id} has {self.reply_count} replies, {self.retweet_count} retweet(s), " \
               f"{self.quote_count} quote(s) and {self.like_count} like(s)"

    def sum_metric_count(self):
        return self.reply_count + self.quote_count


class User:
    def __init__(self, user_id):
        self.id = user_id
        self.username = str
        self.tweets = []
        self.retrieved = False

    def __repr__(self):
        return f"User {self.username} with id {self.id} published the following tweets {self.tweets}"

    def add_tweet(self, tweet):
        self.tweets.append(tweet)

    def set_username(self, username):
        self.username = username


def write_file(response, out_file):
    try:
        # response = response["data"]
        out_file.write(json.dumps(response, indent=4, sort_keys=True))
    except simplejson.errors.JSONDecodeError:
        logger.info("Failed to write response to file")


def process_result(response, f_name):
    logger.info(f'Processing results of {f_name}...')
    tweet_func = {"get_seed", "get_replies", "get_quotes"}
    user_func = {"get_users_by_id", "get_liking_users", "get_retweeting_users"}

    if "data" not in response:
        logger.warning(f"Empty response returned from {f_name} --> skip")
        logger.warning(response)
        return
    response = response["data"]
    for res in response:
        res["seed"] = SEED_TWEET_ID
        res["crawl_timestamp"] = crawl_time_stamp

        if f_name in tweet_func:
            # tweet object
            res["likes_crawled"] = False
            res["retweets_crawled"] = False

            if f_name == "get_quotes":
                # quote
                tweet_cache.append(Tweet(res["id"], res["public_metrics"]))
            if f_name == "get_seed" or f_name == "get_replies":
                # seed or reply
                if res["author_id"] not in author_cache:
                    # new user --> add to cache
                    new_user = User(res["author_id"])
                    new_user.add_tweet(Tweet(res["id"], res["public_metrics"]))
                    author_cache[res["author_id"]] = new_user
                else:
                    # user existing in cache
                    existing_user = author_cache[res["author_id"]]
                    existing_user.add_tweet(Tweet(res["id"], res["public_metrics"]))
        else:
            # user object
            res["followers_crawled"] = False
            res["following_crawled"] = False
            res["timeline_crawled"] = False
            res["liked"] = []
            res["retweeted"] = []

            # regular user response
            author_cache[res["id"]].set_username(res["username"])
            author_cache[res["id"]].retrieved = True

    if f_name in tweet_func:
        collection = "cc_tweets"
    elif f_name in user_func:
        collection = "cc_users"
    else:
        logger.warning(f"No suitable collection in db found for {f_name}")
        collection = "default"
    # insert to db
    db.insert(response, collection)


def recursive_crawl(crawl_function, params):
    time.sleep(.8)  # only 1 request per second allowed (response time + sleep > 1)
    response = crawl_function(**params)
    try:
        remaining = int(response.headers["x-rate-limit-remaining"])
        max_remaining = int(response.headers["x-rate-limit-limit"])
        limit_reset_time = int(response.headers["x-rate-limit-reset"])
        logger.info(f"Remaining: {remaining}")
        logger.info(f"Max requests: {max_remaining}")
        response_time = float(response.headers["x-response-time"]) * 0.001
        # time.sleep(1 - response_time + 1.5 if response_time < 1 else 1.5)
        response_json = response.json()
        if "data" in response_json:
            write_file(response_json, out_file)
            process_result(response_json, crawl_function.__name__)
        else:
            logger.info(response_json) # TODO ABBRUCH BEDINGUNG WENN QUOTE FERTIG
            logger.info("No data in response")
            logger.info("Rate Limit Error on first request --> wait on limit reset")
            return limit_reset_time
        if "meta" in response_json:
            if "next_token" not in response_json["meta"]:
                logger.info("Successfully crawled tweet")
                return None
            elif remaining == 0 or remaining == 2700:  # TODO RATE-LIMIT-BUG BY TWITTER API
                # Next_token available but crawl limit reached
                logger.info(
                    "Crawl Limit reached max crawls: {} next reset time: {}".format(max_remaining, limit_reset_time))
                return limit_reset_time
            else:
                # More results available --> use next_token
                if "meta" in response_json:
                    next_token = response_json["meta"]["next_token"]
                    logger.info("Next crawl --> Pagination token " + next_token)
                    params["next_token"] = next_token
                    return recursive_crawl(crawl_function, params)
        else:
            # user crawl and no limit reached --> continue
            logger.info("Successfully crawled user")
            return None
    except KeyError:
        logger.exception("Error in recursive crawl")
        logger.error(f'{response}')
        logger.error(f'{params}')

def crawl(crawl_function, params):
    next_crawl_time = time.time()
    while next_crawl_time is not None:
        next_crawl_time = recursive_crawl(crawl_function, params)
        logger.info("Next Crawl Time {}".format(next_crawl_time))
        if next_crawl_time is None:
            # Crawl done without exceeding any limits
            break
        logger.info(
            f"Wait until limit reset in {timedelta(seconds=next_crawl_time - int(time.time()))} h/m/s")
        try:
            time.sleep(next_crawl_time - time.time())
        except ValueError():
            logger.info("Limit reset done")


@timeit
def get_seed(tweet_id):
    logger.info("Retrieving seed tweet")
    response = api.get_tweets_by_id([tweet_id]).json()
    write_file(response, out_file)
    process_result(response, get_seed.__name__)
    return response


@timeit
def reply_tree(tweet_id):
    logger.info("Retrieving replies to seed tweet")
    reply_params = {
        "tweet_id": tweet_id,
        "except_fields": None,
        "next_token": None
    }
    crawl(crawl_function=api.get_replies, params=reply_params)


@timeit
def user():
    logger.info("Retrieving user information")
    author_ids = [author.id for author in author_cache.values() if not author.retrieved]
    for author_id_batch in batch(author_ids, 100):
        crawl(crawl_function=api.get_users_by_id, params={"ids": author_id_batch})


@timeit
def quotes():
    logger.info("Retrieve all quotes, likes and retweets")
    for user in list(author_cache.values()):
        for tweet in user.tweets:
            if tweet.quote_count > 0 and not tweet.quotes_retrieved:
                quote_params = {
                    "username": user.username,
                    "tweet_id": tweet.id,
                    "except_fields": None,
                    "next_token": None
                }
                crawl(crawl_function=api.get_quotes, params=quote_params)
                tweet.quotes_retrieved = True
            # todo separate likes retweets due to bandwith and use crawl function for infinite crawling
            # if tweet.like_count > 0 and not tweet.likes_retrieved:
            #     logger.info("Retrieve likes")
            #     like_response = api.get_liking_users(tweet.id).json()
            #     write_file(like_response, out_file)
            #     write_db(like_response, "cc_users", cache=False, reaction=("liked", tweet.id))
            #     tweet.likes_retrieved = True
            # if tweet.retweet_count > 0 and not tweet.retweets_retrieved:
            #     logger.info("Retrieve retweets")
            #     retweet_response = api.get_retweeting_users(tweet.id).json()
            #     write_file(retweet_response, out_file)
            #     write_db(retweet_response, "cc_users", cache=False, reaction=("retweeted", tweet.id))
            #     tweet.retweets_retrieved = True


@timeit
def pipeline(tweet_id):
    """
    Recursive pipeline which retrieves reply tree, involved users and quotes
    @param tweet_id: seed tweet id
    @return: writes results to file and db
    """
    #reply_tree(tweet_id)
    user()
    quotes()
    while len(tweet_cache) > 0:
        twt_obj = tweet_cache.pop(0)
        if twt_obj.sum_metric_count() > 0:
            pipeline(twt_obj.id)


# FOLLOWER OF USER
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

events = {
    -1: "1442243266280370177",  # vanderhorst test tweet
    0: "1433361036191612930",  # toni test tweet
    1: "1158074774297468928"  # neil degrasse tyson
}

out_file = open("output/crawl_tweets.txt", "w")
author_cache = {}
tweet_cache = []

if __name__ == "__main__":
    crawl_time_stamp = datetime.now()
    SEED_TWEET_ID = events[1]
    get_seed(SEED_TWEET_ID)
    pipeline(SEED_TWEET_ID)
