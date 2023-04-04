import json
import os
import tweepy
import boto3
import datetime
import time
import pandas as pd

def lambda_handler(event, context):
    # Connect to Twitter API
    api_key = os.environ['API_KEY']
    api_key_secret = os.environ['API_KEY_SECRET']
    access_token = os.environ['ACCESS_TOKEN']
    access_token_secret = os.environ['ACCESS_TOKEN_SECRET']
    handle = os.environ['TWITTER_HANDLE']
    replies_count = int(os.environ["REPLIES_LAST_RESULT"])
    replies_base_count = int(os.environ["REPLIES_BASE_RESULT"])
    
    # Get data from scraper_main and IDs of Tweets collected from principal handle
    tweet_ids = json.loads(event["body"])
    tweet_ids.sort()
    
    # Set up twitter connection
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    user_name = "@" + handle
    
    # Get replies to tweets per tweet ID
    replies_iter_list = []

    try:
        replies_iter_list.append(tweepy.Cursor(api.search_tweets, q='to:{}'.format(user_name), since_id=min(tweet_ids), max_id=max(tweet_ids), tweet_mode='extended').items(replies_count))
        replies_iter_list.append(tweepy.Cursor(api.search_tweets, q='to:{}'.format(user_name), since_id=max(tweet_ids), tweet_mode='extended').items(replies_base_count))
    except tweepy.error.RateLimitError:
        # hit rate limit, sleep for 15 minutes
        print('Rate limited. Sleeping for 15 minutes.')
        time.sleep(15 * 60 + 15)
        
    # Extract and process ItemIterator containing replies, to a list of replies in JSON format
    json_replies_all = []
    for replies_iter in replies_iter_list:
        hasNext = True
        while (hasNext):
            try:
                reply = replies_iter.next()
                json_replies_all.append(reply.__dict__["_json"])
            except StopIteration:
                hasNext = False
                
    # Drop some rows
    df = pd.json_normalize(json_replies_all)
    df = df[df['in_reply_to_status_id'].notna()]
    df[df["in_reply_to_status_id"].isin(tweet_ids)]

    '''
        Upload to S3
    '''
    date_time = datetime.datetime.now().strftime("%m%d%Y")
    FOLDER_NAME = date_time + "/"
    FILE_NAME = "replies-data.json"

    client = boto3.client('s3')
    body = df.to_json(orient="records")
    client.put_object(
        Bucket = os.environ['BUCKET_NAME'], 
        Key=FOLDER_NAME + FILE_NAME,
        Body=body
    )
    
    client.put_object(
        Bucket = os.environ['BUCKET_NAME'],
        Key="latest-replies/" + FILE_NAME,
        Body=body
    )

    return {
        'statusCode': 200,
        'body': json.dumps("Replies collected."),
        'TYPE': "PASS"
    }
