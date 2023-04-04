# IMPORTANT!!!!!
# Create a bucket called sentimenandtopic
# Create a folder called output in sentimenandtopic bucket

# This code is a Lambda function written in Python that uses Amazon Comprehend to detect sentiments in texts json uploaded to an S3 bucket called sentimenandtopic. The function is triggered by an S3 event, which passes the S3 bucket name or sentimenandtopic and object key of the uploaded .json to the function.
# The function then uses the Boto3 library to call Amazon Comprehend to detect sentiments and topic in the "comment" json. The labels and their confidences are logged to the CloudWatch logs and written to a JSON file in the output folder in the same S3 bucket with the same object key appended with ".json". Finally, the function returns the sentiments and topics detected in the text.
# This code can be used for topic and sentiment analysis.
import json
import boto3
from datetime import datetime
import nltk

REGION = "us-east-1"

# This code can be used for topic and sentiment analysis.
#can do a bit of cleaning first to enhance matching


def lambda_handler(event, context):
    nltk.data.path.append("/tmp")
    nltk.download("stopwords", download_dir="/tmp")
    nltk.download('wordnet', download_dir="/tmp")
    
    from nltk.stem import WordNetLemmatizer

    s3 = boto3.client("s3")
    comprehend = boto3.client("comprehend", region_name=REGION)

    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_object = event["Records"][0]["s3"]["object"]["key"]
    print("S3 Bucket: ", s3_bucket)
    print("S3 Object: ", s3_object)

    # Get the contents of the file
    response = s3.get_object(Bucket=s3_bucket, Key=s3_object)
    contents = response["Body"].read().decode("utf-8")
    
    data = json.loads(contents)

    # Detect the sentiment and topics for each comment
    results = []
    
    for category in data:
        data_=data[category]
        print(len(data_))
        for comment in data_:
            sentiment_response = comprehend.detect_sentiment(
                Text=comment["clean_text"], LanguageCode="en")
            topic_response = comprehend.detect_key_phrases(
                Text=comment["clean_text"], LanguageCode="en")
                
            #process topic response to 1. break into single words (which hopefully repeat more) 2. remove excess spaces
            #3. remove stop words
            key_phrases_dict = topic_response["KeyPhrases"]
            phrases_list = []
            for key_phrase in key_phrases_dict:
                phrases_list+=key_phrase["Text"].lower().split()
            topic_response = list(set(phrases_list) - set(nltk.corpus.stopwords.words('english')))
            topic_response = [WordNetLemmatizer().lemmatize(word) for word in topic_response]
    
            # Log the sentiment and topics to CloudWatch
            print(sentiment_response)
            print(topic_response)
    
            # Save the sentiment and topics to S3
            output_body = {
                #ADDED TWEET ID IDENTIFIER ----------
                "id_str": str(comment["id"]),
                "sentiment": sentiment_response,
                "topics": topic_response,
            }
            # Add the sentiment and topics to the results list
            results.append(output_body)

    jack_date = s3_object.split('-')[-1]
    output_key = "sentiment_topic/"+ jack_date
    #PUT 1 JSON FILE IN PER DAY ONLY ---------- name of file is date of processing of bot
    s3.put_object(Bucket=s3_bucket, Key=output_key, Body=json.dumps(results))

    # Return the sentiment and topics for all comments
    return results
