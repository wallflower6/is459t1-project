import json
import os
import tweepy
import datetime
import requests
import pandas as pd
import boto3

def lambda_handler(event, context):
    past_n_days = int(os.environ["PAST_N_DAYS"])
    
    # Connect to Twitter API
    BEARER_TOKEN = os.environ["BEARER_TOKEN"]
    client = tweepy.Client(bearer_token=BEARER_TOKEN, return_type=requests.Response)
    handle = os.environ['TWITTER_HANDLE']
    user_name = "@" + handle
    
    # Tweet criteria - start date
    today = datetime.datetime.now()
    d = datetime.timedelta(days = past_n_days)
    start_time = today - d
    start_time = start_time.strftime("%Y-%m-%dT%H:%M:00Z")
    
    # Get tweets
    handle = 'ZelenskyyUa'
    query = 'from:' + handle + ' -is:retweet'
    tweet_fields = ["created_at","id", "lang",'context_annotations', 'entities', 'author_id', 'conversation_id', 'in_reply_to_user_id', 'public_metrics']
    user_fields = ["id", "username"]
    
    tweets = client.search_recent_tweets(
                query=query,
                tweet_fields=tweet_fields,
                user_fields=user_fields,
                start_time=start_time,
                max_results=100)
                
    # Save data as dictionary
    tweets_dict = tweets.json()

    # Extract "data" value from dictionary
    tweets_data = tweets_dict['data'] 

    # Transform to pandas Dataframe
    df = pd.json_normalize(tweets_data)
    
    print(type(df["created_at"].tolist()[0]))
    
    tweet_ids = df["id"].tolist()
    if (len(tweet_ids) <= 0):
        return {
            'TYPE': 'NO_TWEETS',
            'statusCode': 200,
            'body': json.dumps("No new tweets found.")
        }
    
    '''
        Upload to S3
    '''
    date_time = datetime.datetime.now().strftime("%m%d%Y")
    FOLDER_NAME = date_time + "/"
    FILE_NAME = "tweets-data.json"

    client = boto3.client('s3')
    client.put_object(
        Bucket = os.environ['BUCKET_NAME'], 
        Key=FOLDER_NAME + FILE_NAME,
        Body=df.to_json(orient="records")
    )

    return {
        'TYPE': 'TWEETS',
        'statusCode': 200,
        'body': json.dumps(tweet_ids)
    }