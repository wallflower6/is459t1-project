import json
import boto3
import pandas as pd
import os
import googletrans
from googletrans import Translator
import datetime

def lambda_handler(event, context):
    
    '''
    Get data from S3
    '''
    s3 = boto3.client('s3')
    s3_data = s3.get_object(
        Bucket=os.environ["BUCKET_NAME"],
        Key="transit/botsdetected.json"
    )
    
    '''
    Convert to pandas dataframe
    '''
    data = json.loads(s3_data.get("Body").read().decode("utf-8"))
    dfTweets = pd.DataFrame(data["tweets"])
    dfReplies = pd.DataFrame(data["replies"])
    
    '''
    Initialize APIs
    '''
    # initialize translator
    translator = Translator()
    valid_languages = googletrans.LANGUAGES.keys()
    
    # Translate a record if lang is valid but not english
    def translate(df, i, translator, valid_languages):
        lang = df.at[i, 'lang']
        if lang != 'en':
            if lang in valid_languages:
                df.at[i, "clean_text"] = translator.translate(df.at[i, "clean_text"], dest="en", src=lang).text
            else:
                df.drop(i, inplace=True)
                
    '''
    Translate
    '''
    for i, row in dfTweets.iterrows():
        translate(dfTweets, i, translator, valid_languages)
        
    for i, row in dfReplies.iterrows():
        translate(dfReplies, i, translator, valid_languages)
    
    '''
    Save output to JSON and S3
    '''
    op = {"tweets": dfTweets.to_dict('records'), "replies": dfReplies.to_dict('records')}
    client = boto3.client('s3')
    
    FOLDER_NAME = "translated_factchecked/"
    date_time = datetime.datetime.now().strftime("%m%d%Y")
    FILE_NAME = "translated-" + date_time + ".json"
    body = json.dumps(op)
    client.put_object(
        Bucket = os.environ['BUCKET_NAME'], 
        Key=FOLDER_NAME + FILE_NAME,
        Body=body
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Translator ran')
    }
