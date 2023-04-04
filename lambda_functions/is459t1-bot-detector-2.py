import json
import boto3
import pandas as pd
import os
import botometer
import datetime

def lambda_handler(event, context):
    
    payload = json.loads(event["body"])
    index = payload["last_index"]
    accounts_to_drop = payload["accounts_to_drop"]
    
    '''
    Get data from S3
    '''
    s3 = boto3.client('s3')
    s3_data = s3.get_object(
        Bucket=os.environ["BUCKET_NAME"],
        Key="transit/clean-tweets-data.json"
    )
    
    '''
    Convert to pandas dataframe
    '''
    data = json.loads(s3_data.get("Body").read().decode("utf-8"))
    dfTweets = pd.DataFrame(data["tweets"])
    dfReplies = pd.DataFrame(data["replies"])
    
    '''
    Initialize Botometer
    '''
    rapidapi_key = os.environ["RAPID_API_KEY"]
    twitter_app_auth = {
        'consumer_key': os.environ["TWITTER_CONSUMER_KEY"],
        'consumer_secret': os.environ["TWITTER_CONSUMER_SECRET"]
    }
    bom = botometer.Botometer(wait_on_ratelimit=True,
                            rapidapi_key=rapidapi_key,
                            **twitter_app_auth)
    
    '''
    Run through Replies
    '''
    accounts = dfReplies["user.screen_name"].tolist()
    try:
        for i in range(last_index, len(accounts)):
            result = bom.check_account('@' + accounts[i])
            localizations = result["display_scores"]
            for localization in localizations:
                metrics = localizations[localization]
                for metric in metrics:
                    if metrics[metric] > 3.5:
                        accounts_to_drop.append(accounts[i])
    except:
        # Save Output
        dfReplies = dfReplies[~dfReplies["user.screen_name"].isin(accounts_to_drop)]
        op = {"tweets": dfTweets.to_dict('records'), "replies": dfReplies.to_dict('records')}
        
        client = boto3.client('s3')
        date_time = datetime.datetime.now().strftime("%m%d%Y")
        FOLDER_NAME = "transit/"
        FILE_NAME = "botsdetected.json"
        body = json.dumps(op)
        
        client.put_object(
            Bucket = os.environ['BUCKET_NAME'],
            Key=FOLDER_NAME + FILE_NAME,
            Body=body
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps("BotDetector2 hit rate limit, saving to S3.")
        }
    
    # Save Output
    dfReplies = dfReplies[~dfReplies["user.screen_name"].isin(accounts_to_drop)]
    op = {"tweets": dfTweets.to_dict('records'), "replies": dfReplies.to_dict('records')}
    
    client = boto3.client('s3')
    date_time = datetime.datetime.now().strftime("%m%d%Y")
    FOLDER_NAME = date_time + "/bot/"
    FILE_NAME = "botsdetected.json"
    body = json.dumps(op)
    
    client.put_object(
        Bucket = os.environ['BUCKET_NAME'],
        Key=FOLDER_NAME + FILE_NAME,
        Body=body
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps("BotDetector2 ran to completion.")
    }