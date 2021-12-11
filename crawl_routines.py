import json
import queue
from datetime import datetime, timedelta
import simplejson.errors
import mongo_db as db
from test_api import ApiEndpoints
from helper import *
from threading import Thread

api = ApiEndpoints()

tweet_func = {"get_seed", "get_replies", "get_quotes"}
user_func = {"get_users_by_id", "get_liking_users", "get_retweeting_users"}
timeline_func = "get_timeline"
follow_func = {"get_followers", "get_following"}
reaction_func = {"get_liking_users", "get_retweeting_users"}

TIMELINE_COLLECTION = "cc_timelines"


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
        self.user_retrieved = False

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


def process_result(response, f_name, params=None):
    logger.info(f'Processing results of {f_name}...')
    if "data" not in response:
        logger.warning(f"Empty response returned from {f_name} --> skip")
        logger.warning(response)
        return
    response = response["data"]
    if f_name == timeline_func:
        logger.info(f"Inserting timeline tweets to db {TIMELINE_COLLECTION}")
        db.insert(response, TIMELINE_COLLECTION)
        return
    if f_name in reaction_func:
        logger.info(f"Inserting reaction to tweets ino db")
        update_field = "liked" if f_name == "get_liking_users" else "retweeted"
        for res in response:
            found_user = db.read({"id": res["id"]}, "cc_users")
            if len(list(found_user)) > 0:
                logger.info("Found user in DB")
                db.update_array(res["id"], update_field, params["tweet_id"], "cc_users")
            else:
                logger.info(f"New user {res['id']}--> Create and update")
                res["liked"] = []
                res["retweeted"] = []
                db.insert([res], "cc_users")
                db.update_array(res["id"], update_field, params["tweet_id"], "cc_users")
        return
    if f_name in follow_func:
        logger.info(f"Inserting followers/following of users ino db")
        update_field = "following" if f_name == "get_followers" else "followed_by"
        for res in response:
            found_user = db.read({"id": res["id"]}, "cc_follows")
            if len(list(found_user)) > 0:
                logger.info("Found user in DB")
                db.update_array(res["id"], update_field, params["user_id"], "cc_follows")
            else:
                logger.info(f"New user {res['id']}--> Create and update")
                res["following"] = []
                res["followed_by"] = []
                db.insert([res], "cc_follows")
                db.update_array(res["id"], update_field, params["user_id"], "cc_follows")
        return
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
            author_cache[res["id"]].user_retrieved = True

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
    # logger.info(f"Crawling function {crawl_function.__name__} params: {params}")
    stack = [(crawl_function, params)]
    while len(stack) > 0:
        func = stack.pop(0)
        response = func[0](**func[1])
        try:
            remaining = int(response.headers["x-rate-limit-remaining"])
            max_remaining = int(response.headers["x-rate-limit-limit"])
            limit_reset_time = int(response.headers["x-rate-limit-reset"])
            logger.info(f"Remaining: {remaining}")
            logger.info(f"Max requests: {max_remaining}")
            response_time = float(response.headers["x-response-time"]) * 0.001
            if crawl_function.__name__ in tweet_func:  # according to documentation sleep only needed for full archive search
                # only 1 request per second allowed (response time + sleep > 1)
                time.sleep(1 - response_time if response_time < 1 else 0.8)
            response_json = response.json()
            if "data" in response_json:
                write_file(response_json, out_file)
                process_result(response_json, crawl_function.__name__, params=params)
            else:
                logger.info(f"No data --> response: {response_json}")
                if "meta" in response_json:
                    if "result_count" in response_json["meta"]:
                        logger.info("No data in response --> result-count = 0")
                        return None
                elif "errors" in response_json:
                    if "title" in response_json["errors"][0]:
                        if "Not Found Error" == response_json["errors"][0]["title"]:
                            logger.warning("Tweet or User not found --> Skip")
                            return None
                else:
                    logger.info("Rate Limit Error on first request --> wait on limit reset")
                return limit_reset_time
            if "meta" in response_json:
                if "next_token" not in response_json["meta"]:
                    logger.info("Successfully crawled tweet")
                    return None
                elif remaining == 0 or remaining == 2700:  # TODO RATE-LIMIT-BUG BY TWITTER API
                    # Next_token available but crawl limit reached
                    logger.info(
                        "Crawl Limit reached max crawls: {} next reset time: {}".format(max_remaining,
                                                                                        limit_reset_time))
                    return limit_reset_time
                else:
                    # More results available --> use next_token
                    if "meta" in response_json:
                        if crawl_function.__name__ in follow_func:
                            # Follower crawl --> Don't use next_token due to rate-limits --> crawling max 1000 followers
                            return None
                        next_token = response_json["meta"]["next_token"]
                        logger.info(f"Next crawl --> Next token {next_token}"
                                    f"{params}")
                        params["next_token"] = next_token
                        stack.append((crawl_function, params))
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
        logger.info(f"Next Crawl Time {next_crawl_time}")
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
    author_ids = [author.id for author in author_cache.values() if not author.user_retrieved]
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
            else:
                tweet.quotes_retrieved = True


@timeit
def pipeline(tweet_id):
    """
    Recursive pipeline which retrieves reply tree, involved users and quotes
    @param tweet_id: seed tweet id
    @return: writes results to file and db
    """
    try:
        stack = [tweet_id]
        while len(stack) > 0:
            curr_tweet_id = stack.pop(0)
            time.sleep(0.8)
            reply_tree(curr_tweet_id)
            user()
            quotes()
            while len(tweet_cache) > 0:
                twt_obj = tweet_cache.pop(0)
                if twt_obj.sum_metric_count() > 0:
                    stack.append(twt_obj.id)
    except Exception as e:
        logger.error("Error in pipeline")
        logger.exception(e)


@timeit
def threaded_crawl(crawl_function, search_results, target_field_name, num_threads):
    job_queue = queue.Queue()
    for elem in search_results:
        job_queue.put(elem)
    for i in range(num_threads):
        logger.info(f"Main: create and start thread {i}")
        Thread(target=worker, args=(job_queue, crawl_function, target_field_name,), daemon=True).start()
    job_queue.join()


def worker(job_queue, crawl_function, field_name):
    while True:
        new_job = job_queue.get()
        execute_and_modify(crawl_function, new_job, field_name)
        job_queue.task_done()


@timeit
def crawl_retweets():
    target_field_name = "retweets_crawled"
    result = db.read({target_field_name: False, "public_metrics.retweet_count": {"$gt": 0}}, "cc_tweets")
    remaining = db.read({target_field_name: False, f"public_metrics.retweet_count": {"$eq": 0}}, "cc_tweets")
    for remain in remaining:
        db.modify({"id": remain["id"]}, {"$set": {target_field_name: True}}, "cc_tweets")
    threaded_crawl(api.get_retweeting_users, result, target_field_name, num_threads=75)


@timeit
def crawl_likes():
    target_field_name = "likes_crawled"
    result = db.read({target_field_name: False, "public_metrics.like_count": {"$gt": 0}}, "cc_tweets")
    remaining = db.read({target_field_name: False, f"public_metrics.like_count": {"$eq": 0}}, "cc_tweets")
    for remain in remaining:
        db.modify({"id": remain["id"]}, {"$set": {target_field_name: True}}, "cc_tweets")
    threaded_crawl(api.get_liking_users, result, target_field_name, num_threads=75)


@timeit
def crawl_timelines():
    target_field_name = "timeline_crawled"
    result = db.read({target_field_name: False}, "cc_users")
    threaded_crawl(api.get_timeline, result, target_field_name, num_threads=1500)


@timeit
def crawl_follows():
    target_field_name = "followers_crawled"
    result = db.read({target_field_name: False}, "cc_users")
    threaded_crawl(api.get_followers, result, target_field_name, num_threads=15)


@timeit
def crawl_following():
    target_field_name = "following_crawled"
    result = db.read({target_field_name: False}, "cc_users")
    threaded_crawl(api.get_following, result, target_field_name, num_threads=15)


def execute_and_modify(crawl_function, db_response, field_name):
    """
    Actually executes crawl function and modifies db to write back status on crawling
    @param crawl_function: to be executed - namely retweet, like or timeline crawl
    @param db_response: which returned ids to be crawled with @crawl_function
    @param field_name: of document in db which has to be set to true after successful crawl
    """
    params = {"except_fields": None}
    if field_name in {"retweets_crawled", "likes_crawled"}:
        params["tweet_id"] = db_response["id"]
        collection = "cc_tweets"
    elif field_name in {"timeline_crawled", "followers_crawled", "following_crawled"}:
        params["user_id"] = db_response["id"]
        params["next_token"] = None
        collection = "cc_users"
    else:
        logger.error("Field name for db modification unknown")
        raise Exception
    crawl(crawl_function, params)
    db.modify({"id": db_response["id"]}, {"$set": {field_name: True}}, collection)


class EventSearch:
    def __init__(self, uid, tweet_id, start_date, hashtag, username, comment):
        self.uid = uid
        self.tweet_id = tweet_id
        self.start_date = start_date
        self.hashtag = hashtag
        self.username = username
        self.comment = comment

    def __repr__(self):
        return f"{self.tweet_id} + {self.username} + {self.uid}"


event_list = [
    EventSearch(-1, "1442243266280370177", start_date=None, hashtag=None, username=None, comment="vanderhorst"),
    EventSearch(0, "1433361036191612930", start_date=None, hashtag=None, username=None, comment="toni test"),
    EventSearch(1, tweet_id="1158074774297468928", start_date="2019-08-03T23:59:59.000Z", hashtag=["neildegrassetyson"],
                username="neiltyson", comment="neil de grasse tyson"),
]

out_file = open("output/crawl_tweets.txt", "w")
author_cache = {}
tweet_cache = []

if __name__ == "__main__":
    crawl_time_stamp = datetime.now()
    event = event_list[1]
    SEED_TWEET_ID = event.tweet_id
    get_seed(SEED_TWEET_ID)
    pipeline(SEED_TWEET_ID)
    crawl_likes()
    crawl_retweets()
    crawl_timelines()
    crawl_following()
    crawl_follows()
