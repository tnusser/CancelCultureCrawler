import datetime
import json
import time
import simplejson.errors
import test_api

crawler = test_api.ApiEndpoints()


def write_db(response, out_file):
    try:
        out_file.write(json.dumps(response, indent=4, sort_keys=True))
    except simplejson.errors.JSONDecodeError:
        test_api.logger.info("Rate Limit Error on first request --> wait on limit reset")
        return int(response.headers["x-rate-limit-reset"])


def recursive_crawl(crawl_function, params, out_file):
    time.sleep(0.5)
    response = crawl_function(**params)
    remaining = int(response.headers["x-rate-limit-remaining"])
    max_remaining = int(response.headers["x-rate-limit-limit"])
    limit_reset_time = int(response.headers["x-rate-limit-reset"])
    try:
        res_json = response.json()
        write_db(res_json, out_file)
    except simplejson.errors.JSONDecodeError:
        test_api.logger.info("Rate Limit Error on first request --> wait on limit reset")
        return limit_reset_time
    if "next_token" not in res_json["meta"]:
        test_api.logger.info("Successfully crawled ")
        return None
    elif remaining == 0:
        # Next_token available but crawl limit reached
        test_api.logger.info(
            "Crawl Limit reached max crawls: {} next reset time: {}".format(max_remaining, limit_reset_time))
        return limit_reset_time
    else:
        # More results available --> use next_token
        next_token = res_json["meta"]["next_token"]
        test_api.logger.info("Next crawl --> Pagination token " + next_token)
        params["next_token"] = next_token
        return recursive_crawl(crawl_function, params, out_file)


def crawl(crawl_function, params, out_file):
    next_crawl_time = time.time()
    while next_crawl_time is not None:
        next_crawl_time = recursive_crawl(crawl_function, params, out_file)
        test_api.logger.info("Next Crawl Time {}".format(next_crawl_time))
        if next_crawl_time is None:
            # Crawl done without exceeding any limits
            break
        test_api.logger.info(
            "Wait until limit reset in " + str(
                datetime.timedelta(seconds=next_crawl_time - int(time.time()))) + " h/m/s")
        try:
            time.sleep(next_crawl_time - time.time())
        except ValueError():
            test_api.logger.info("Limit reset done")
