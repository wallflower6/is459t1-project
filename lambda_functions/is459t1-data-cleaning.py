import pandas as pd
import numpy as np
import boto3
import json
import emoji
import re
import os
import datetime

def lambda_handler(event, context):
    '''
    Data Cleaning Helper Functions
    '''
    def remove_user_mentions(text):
        return re.sub("@[A-Za-z0-9_]+","", text)
    
    def remove_links(text):
        return re.sub(r'http\S+', '', text)
    
    def remove_digit_strings(text):
        return re.sub(r'\d+', '', text)
        
    def remove_newline(text):
        return ' '.join(x.strip() for x in text.split('\n'))
    
    def remove_special_chars(text):
        # remove_chars = '[0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        remove_chars = '["#$%&\()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        return re.sub(remove_chars, ' ', text)
        
    def convert_emojis(text):
        return emoji.demojize(text, language='alias')
    
    def clean_text(text):
        result_text = text
        result_text = remove_user_mentions(result_text)
        result_text = remove_links(result_text)
        # result_text = remove_digit_strings(result_text)
        result_text = remove_newline(result_text)
        result_text = remove_special_chars(result_text)
        result_text = convert_emojis(result_text)
        result_text = result_text.lower()
        return result_text
        
    '''
    Get data from S3
    '''
    filename = datetime.datetime.now().strftime("%m%d%Y")
    s3 = boto3.client('s3')
    tweets_data = s3.get_object(
        Bucket=os.environ["BUCKET_NAME"],
        Key=filename + "/tweets-data.json"
    )
    
    replies_data = s3.get_object(
        Bucket=os.environ["BUCKET_NAME"],
        Key=filename + "/replies-data.json"
    )
    
    '''
    Convert to pandas dataframe
    '''
    tweets_json_data = json.loads(tweets_data.get("Body").read().decode("utf-8"))
    replies_json_data = json.loads(replies_data.get("Body").read().decode("utf-8"))
    
    tweets_df = pd.DataFrame.from_records(tweets_json_data)
    replies_df = pd.DataFrame.from_records(replies_json_data)
    
    tweets_texts = tweets_df["text"].tolist()
    replies_texts = replies_df["full_text"].tolist()
    
    # Clean tweets
    for i in range(len(tweets_texts)):
        tweets_texts[i] = clean_text(tweets_texts[i])
        if (tweets_texts[i] == "" or tweets_texts[i].isspace()):
            tweets_texts[i] = np.nan
            
    # Clean replies
    for i in range(len(replies_texts)):
        replies_texts[i] = clean_text(replies_texts[i])
        if (replies_texts[i] == "" or replies_texts[i].isspace()):
            replies_texts[i] = np.nan
    
    # Add cleared text as new column
    tweets_df["clean_text"] = tweets_texts
    replies_df["clean_text"] = replies_texts
    
    # Drop empty rows
    tweets_df.dropna(subset=['clean_text'], inplace=True)
    replies_df.dropna(subset=['clean_text'], inplace=True)
    
    # Replace NaN values with string
    tweets_df.fillna('', inplace=True)
    replies_df.fillna('', inplace=True)
    
    '''
    Put both dataframes in a single JSON file
    '''
    tweets_df_json_str = json.loads(tweets_df.to_json(orient='records'))
    replies_df_json_str = json.loads(replies_df.to_json(orient='records'))
    
    combined = {}
    combined["tweets"] = tweets_df_json_str
    combined["replies"] = replies_df_json_str
    
    '''
    Convert to JSON
    Upload to clean folder in S3
    Upload to latest-clean-replies folder in S3
    '''
    date_time = datetime.datetime.now().strftime("%m%d%Y")
    FOLDER_NAME = date_time + "/clean/"
    FILE_NAME = "clean-tweets-data.json"
    
    body = json.dumps(combined)
    s3.put_object(
        Bucket = os.environ['BUCKET_NAME'], 
        Key=FOLDER_NAME + FILE_NAME,
        Body=body
    )

    s3.put_object(
        Bucket = os.environ['BUCKET_NAME'],
        Key="transit/" + FILE_NAME,
        Body=body
    )
    
    '''
    Trigger Step Function
    '''
    sf = boto3.client('stepfunctions', region_name = os.environ["REGION_NAME"])
    payload = {}
    response = sf.start_execution(
        stateMachineArn=os.environ["STEP_FUNCTION_ARN"],
        input=json.dumps(payload)
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
