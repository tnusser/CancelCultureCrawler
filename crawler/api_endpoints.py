import configparser
from distutils.util import strtobool

import requests
import os
import re
from utils import logger

config = configparser.ConfigParser()
config.read("../config.ini")

logger.info("-------------------")
logger.info("Start crawl routine")
logger.info("-------------------")

API_BASE_URL = "https://api.twitter.com/2/"

if "TWITTER_BEARER_TOKEN" in os.environ:
    BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN")
else:
    BEARER_TOKEN = config["twitter"]["BearerToken"]


class ApiEndpoints:
    def __init__(self):
        self.TWEET_FIELDS = ["attachments", "author_id", "conversation_id", "created_at",
                             "entities", "geo", "id", "in_reply_to_user_id", "lang", "possibly_sensitive",
                             "public_metrics", "referenced_tweets", "reply_settings", "source", "text", "withheld"]
        self.USER_FIELDS = ["created_at", "description", "entities", "id", "location", "name", "pinned_tweet_id",
                            "profile_image_url", "protected", "public_metrics", "url", "username", "verified",
                            "withheld"]
        self.HEADER = {"Authorization": "Bearer {}".format(BEARER_TOKEN)}
        self.START_DATE = "2010-01-20T23:59:59.000Z"

        self.NER = config["twitter"]["NamedEntityRecognition"]
        if bool(strtobool(self.NER)):
            self.TWEET_FIELDS.append("context_annotations")

    @staticmethod
    def except_fields(field_name, base_fields, params, except_fields=None):
        """
        Processes parameter dictionary and appends or removes certain fields
        @param field_name:
        @param base_fields:
        @param params: dictionary of incoming parameters
        @param except_fields: fields that should be removed from the parameter dict
        @return: params dictionary
        """
        if except_fields is None:
            wanted_fields = ",".join(base_fields)
            params[field_name] = wanted_fields
        elif except_fields == "default":
            # Retrieve default fields
            pass
        else:
            # Remove unwanted fields
            unwanted_fields = re.split(",", except_fields)
            wanted_fields = ",".join([f for f in base_fields if f not in unwanted_fields])
            params[field_name] = wanted_fields
        return params

    def get_tweets_by_id(self, ids, except_fields=None):
        """
        Retrieves tweet objects
        :param ids: comma separated list of user ids to be retrieved (max: 100)
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        :return: json object containing requested fields of the tweet
        """
        if len(ids) > 100:
            logger.error("get_users_by_id called with more than 100 users")
            return
        params = {"ids": ",".join(ids)}
        params = self.except_fields("tweet.fields", self.TWEET_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "tweets?", params=params, headers=self.HEADER)
        return self.exception_handler(response)

    def get_tweets_by_hashtag_or_mention(self, hashtags_or_mentions, start_date, end_date, except_fields=None,
                                         next_token=None):
        """
        Retrieves all tweets containing certain hashtags or user mentions
        @param hashtags_or_mentions: set of hashtags in the following format hashtags: #myhashtag mentions: @myuser
        @param start_date: start date from which tweets are crawled in the format yyyy-mm-ddT23:59:59.000Z
        @param end_date: end date from which tweets are crawled in the format yyyy-mm-ddT23:59:59.000Z
        @param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        @param next_token: token used to retrieve results using pagination
        @return: json object containing requested fields of the tweet
        """
        q_string = ""
        for h_or_m in hashtags_or_mentions:
            q_string += f'{h_or_m} -is:retweet OR '
        q_string = q_string.strip(" OR ")
        params = {
            'query': f"{q_string}",
            'start_time': start_date,
            'end_time': end_date,
            'max_results': 500
        }
        response = self.full_archive_search(next_token, params, except_fields)
        return self.exception_handler(response)

    def get_users_by_id(self, ids, except_fields=None):
        """
        Retrieves user profiles
        :param ids: comma separated list of user ids to be retrieved (max: 100)
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, name and username
        :return: json object containing requested fields of the user profile
        """
        if len(ids) > 100:
            logger.error("get_users_by_id called with more than 100 users")
            return
        params = {"ids": ",".join(ids)}
        params = self.except_fields("user.fields", self.USER_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "users?", params=params, headers=self.HEADER)
        return self.exception_handler(response)

    def get_timeline(self, user_id, except_fields=None, next_token=None):
        """
        TODO Add Timeline Crawl via Full-Archive-Search
        Retrieves tweets of a users timeline -- X-RATE-LIMIT 1.500 -> 150.000 tweets
        04.10.21 min_results: 5 max_results: 100
        :param user_id id of user to be crawled
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        :param next_token: token used to retrieve results using pagination
        :return: json object containing id and text of tweets (and next_token if results > max_results)
        """
        max_results = "100"
        params = {
            'max_results': max_results
        }
        if next_token is not None:
            params["pagination_token"] = next_token
        params = self.except_fields("tweet.fields", self.TWEET_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "users/" + user_id + "/tweets?", params=params,
                                headers=self.HEADER)
        return self.exception_handler(response)

    def get_followers(self, user_id, except_fields=None, next_token=None):
        """
        Retrieves users that follow the input user
        04.10.21 min_results: 10 max_results: 1000
        :param user_id: id of user to be crawled
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, name and username
        :param next_token: token used to retrieve results using pagination
        :return: json object containing id, name and username of users (and next_token if results > max_results)
        """
        max_results = "1000",
        params = {
            'max_results': max_results
        }
        if next_token is not None:
            params["pagination_token"] = next_token
        params = self.except_fields("user.fields", self.USER_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "users/" + user_id + "/followers?", params=params, headers=self.HEADER)
        return self.exception_handler(response)

    def get_following(self, user_id, except_fields=None, next_token=None):
        """
        Retrieves users that the user(_id) follows
        04.10.21 min_results: 10 max_results: 1000
        :param user_id: id of user to be crawled
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, name and username
        :param next_token: token used to retrieve results using pagination
        :return: json object containing id, name and username of users (and next_token if results > max_results)
        """
        max_results = "1000"
        params = {
            'max_results': max_results
        }
        if next_token is not None:
            params["pagination_token"] = next_token
        params = self.except_fields("user.fields", self.USER_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "users/" + user_id + "/following?", params=params, headers=self.HEADER)
        return self.exception_handler(response)

    def get_liking_users(self, tweet_id, except_fields=None):
        """
        Retrieves the last 100 users who liked the input tweet - rate limit 3000 tweets per 15 min
        :param tweet_id: id of tweet whose likes should be retrieved
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, name and username
        :return: json object containing id, name and username of users that liked that tweet
        """
        params = {}
        params = self.except_fields("user.fields", self.USER_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "tweets/" + tweet_id + "/liking_users?", params=params,
                                headers=self.HEADER)
        return self.exception_handler(response)

    def get_liked_tweets(self, user_id, except_fields=None, next_token=None):
        """
        Retrieves tweets a user liked. Using id of user to be crawled -- X-RATE-LIMIT 75 -> 7.500 tweets
        04.10.21 min_results: 5 max_results: 100
        :param user_id: id of user to be crawled
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        :param next_token: token used to retrieve results using pagination
        :return: json object containing id and text of tweets (and next_token if results > max_results)
        """
        max_results = "100"
        params = {
            'max_results': max_results
        }
        if next_token is not None:
            params["pagination_token"] = next_token
        params = self.except_fields("tweet.fields", self.TWEET_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "users/" + user_id + "/liked_tweets?", params=params,
                                headers=self.HEADER)
        return self.exception_handler(response)

    def get_retweeting_users(self, tweet_id, except_fields=None):
        """
        Retrieves the last 100 users who retweeted the input tweet -- rate limit 3000 tweets per 15 min
        :param tweet_id: id of tweet whose retweets should be retrieved
        :param except_fields: optional param for fields which should be excluded. 'default' --> id, name and username
        :return: json object containing id, name and username of users that retweeted that tweet
        """
        params = {}
        params = self.except_fields("tweet.fields", self.TWEET_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "tweets/" + tweet_id + "/retweeted_by?", params=params,
                                headers=self.HEADER)
        return self.exception_handler(response)

    def get_replies(self, tweet_id, except_fields=None, next_token=None):
        """
        Retrieves conversation tree from tweet id. Per crawl it returns up to 100 replies
        @param tweet_id: id of tweet whose replies should be retrieved
        @param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        @param next_token: token used to retrieve results using pagination
        @return: json object containing id and text of tweets (and next_token if results > max_results)
        """
        max_results = "100" if self.NER else "500",
        params = {
            'query': "conversation_id:" + tweet_id,
            'start_time': self.START_DATE,
            'max_results': max_results,
            # 'expansions': 'referenced_tweets.id.author_id',
        }
        response = self.full_archive_search(next_token, params, except_fields)
        return self.exception_handler(response)

    def get_quotes(self, username, tweet_id, except_fields=None, next_token=None):
        """
        Retrieves all quotes from a tweet id. Per crawl it returns up to 100 replies
        @param username: the parent tweets authors username
        @param tweet_id: id of tweet whose quotes should be retrieved
        @param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        @param next_token: token used to retrieve results using pagination
        @return: json object containing id and text of tweets (and next_token if results > max_results)
        """
        max_results = "100" if self.NER else "500",
        url = "https://twitter.com/" + username + "/status/" + tweet_id
        params = {
            'query': 'url:' + '"' + url + '" is:quote',
            # 'expansions': 'author_id',
            'start_time': self.START_DATE,
            'max_results': max_results,
        }
        response = self.full_archive_search(next_token, params, except_fields)
        return self.exception_handler(response)

    def get_retweets_archive_search(self, username, tweet_text, except_fields=None, next_token=None):
        """
        Alternative way of retrieving retweets. It uses full-text search to find tweet and retrieve its retweets.
        ATTENTION: May not find retweets because full-text search fails due to encoding issues
        @param username: the parent tweets authors username
        @param tweet_text: text content of parent tweet
        @param except_fields: optional param for fields which should be excluded. 'default' --> id, name and username
        @param next_token: token used to retrieve results using pagination
        @return: json object containing id of retweet (and next_token if results > max_results)
        """
        max_results = "100" if self.NER else "500",
        params = {
            'query': '"' + tweet_text + '" retweets_of:' + username,
            'expansions': 'author_id',
            'start_time': self.START_DATE,
            'max_results': max_results,
        }
        response = self.full_archive_search(next_token, params, except_fields)
        return self.exception_handler(response)

    def full_archive_search(self, next_token, params, except_fields):
        """
        Generic full archive search method that executes reply, quote or retweet crawl
        @param next_token: token used to retrieve results using pagination
        @param params: parameter dictionary for the search
        @param except_fields: optional param for fields which should be excluded. 'default' --> id, text
        @return: json object containing the response by the twitter api
        """
        if next_token is not None:
            params["next_token"] = next_token
        params = self.except_fields("tweet.fields", self.TWEET_FIELDS, params, except_fields)
        response = requests.get(API_BASE_URL + "tweets/search/all?", params=params, headers=self.HEADER)
        return response

    @staticmethod
    def exception_handler(response):
        """
        Handles responses and logs potential exceptions
        :param response: response object for a request which was made
        :return: returns response object
        """
        if response.status_code != 200:
            logger.error(f"Request returned an error: {response.status_code} {response.text}")
        return response
