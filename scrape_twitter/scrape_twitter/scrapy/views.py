from django.http import HttpResponse
import tweepy
import pandas as pd
import json
from django.http import JsonResponse

import logging

logger = logging.getLogger(__name__)

def return_connection():
    """
        With the secret Keys Create a Successful Connection
        :return: return the Api object back for the consumption to required function
    """
    try:
        api_key = "w0u2TyUtAga0dPLAc1huwwGYq"
        api_secret_key = "sO4kBhyLJEgSB2RaOLIH3qAZbfIjS582vyobM8HVRefLhQMF72"
        access_token = "709310096-v2FkS6uT5cOFbQklVwiH3oeqKP7Hkw8lWjs7WQz5"
        access_token_secret = "MGDyKHNeeZFhi9QL6AdkqRLLJtUd9gMjsCyQHxEnL3src"
        auth = tweepy.OAuthHandler(api_key, api_secret_key)
        auth.set_access_token(access_token , access_token_secret )
        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api
    except Exception as inst:
        print("The Exception occurred : {}".format(inst))

def get_retweeters(api, tweet_id: int):
        """
        get the list of user_ids who have retweeted the tweet with id=tweet_it
        :param tweet_id: id of thetweet to get its retweeters
        :return: list of user ids who retweeted the tweeet
        """
        data = api.retweets(tweet_id)
        retweets_list = []
        for retweets in data:
            retweets_list.append([retweets.retweeted_status.id, retweets.retweeted_status.text,retweets.user.name, retweets.user.id,
            retweets.user.screen_name,retweets.retweeted_status.created_at,retweets.user.friends_count])
        retweeted_df = pd.DataFrame(retweets_list, columns=['retweet_id', 'retweet_text','name', 'user_id','screen_name','created_date','friends_count'])
        retweeted_df = retweeted_df.drop_duplicates()
        return retweeted_df
    
def to_bulk(a, size=100):
    """Transform a list into list of list. Each element of the new list is a
    list with size=100 (except the last one).
    """
    r = []
    qt, rm = divmod(len(a), size)
    i = -1
    for i in range(qt):
        r.append(a[i * size:(i + 1) * size])
    if rm != 0:
        r.append(a[(i + 1) * size:])
    return r


def fast_check(api, uids):
    """ Fast check the status of specified accounts.
    Parameters
    ---------------
        api: tweepy API instance
        uids: account ids

    Returns
    ----------
    Tuple (active_uids, inactive_uids).
        `active_uids` is a list of active users and
        `inactive_uids` is a list of inactive uids,
            either supended or deactivated.
    """
    try:
        users = api.lookup_users(user_ids=uids,
                                 include_entities=False)
        active_uids = [u.id for u in users]
        inactive_uids = list(set(uids) - set(active_uids))
        return active_uids, inactive_uids
    except tweepy.TweepError as e:
        if e[0]['code'] == 50 or e[0]['code'] == 63:
            logger.error('None of the users is valid: %s', e)
            return [], inactive_uids
        else:
            # Unexpected error
            raise


def check_inactive(api, uids):
    """ Check inactive account, one by one.
    Parameters
    ---------------
    uids : list
        A list of inactive account

    Returns
    ----------
        Yield tuple (uid, reason). Where `uid` is the account id,
        and `reason` is a string.
    """
    for uid in uids:
        try:
            u = api.get_user(user_id=uid)
            logger.warning('This user %r should be inactive', uid)
            yield (u, dict(code=-1, message='OK'))
        except tweepy.TweepyError as e:
            yield (uid, e[0][0])


def check_one_block(api, uids):
    """Check the status of user for one block (<100). """
    active_uids, inactive_uids = fast_check(api, uids)
    inactive_users_status = list(check_inactive(api, inactive_uids))
    return active_uids, inactive_users_status


def check_status(api, large_uids):
    """Check the status of users for any size of users. """
    active_uids = []
    inactive_users_status = []
    for uids in to_bulk(large_uids, size=1000):
        au, iu = check_one_block(api, uids)
        active_uids += au
        inactive_users_status += iu
    return active_uids, inactive_users_status


def acive_or_inactive(api, large_uids):
    """ The acive_or_inactive function to call check_status. """
    # call check_status
    active_uids, inactive_user_status = check_status(api, large_uids)

def data_preparation():
    api = return_connection()
    tweet_lst=[]
    # Geo location for one city we can add more as an array for all the cities
    geoc="38.9072,-77.0369,1mi" 
    for tweet in tweepy.Cursor(api.search,geocode=geoc).items(300):
        tweetDate = tweet.created_at.date()
        if(tweet.coordinates !=None):
            tweet_lst.append([tweetDate,tweet.id,
                              tweet.coordinates['coordinates'][0],tweet.coordinates['coordinates'][1],
                              tweet.user.screen_name,
                              tweet.user.name, tweet.text, tweet.retweet_count,
                              tweet.user._json['geo_enabled']])
    tweet_df = pd.DataFrame(tweet_lst, columns=['tweet_dt', 'id', 'lat','long','username', 'name', 'tweet','retweet_count','geo'])
    result = tweet_df.to_json(orient='records')
    return result, api

def tweet_data(request):
    
    result, api = data_preparation()
    result = json.loads(result)
    return JsonResponse(result ,safe=False)

def retweets_data(request):
    
    concats = []
    data, api = data_preparation()
    data = json.loads(data)
    retweets_result = {}
    retweet_validate = []
    #count = 0 
    for each in data:
        if each['retweet_count'] > 0 or each['retweet_count'] == True and each['id'] not in retweet_validate:
            print("these are id : ",each['id'])
            retweeted = get_retweeters(api, each['id'])
            print(retweeted)
            if retweeted.empty == False:
                concats.append(retweeted)
                concatenated_df = pd.concat(concats)
                retweets_result = concatenated_df.to_json(orient='records')
                retweet_validate.append(each['id'])
    #retweets_result = json.dumps(retweets_result)
    return JsonResponse(json.loads(retweets_result),safe=False)
            
                
    
        
