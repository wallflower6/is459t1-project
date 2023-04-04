import json
import boto3
import datetime
import os

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    folders = s3.list_objects_v2(
        Bucket=os.environ["BUCKET_NAME"],
    )
    
    '''
        Removal criteria: data older than 7 days
    '''
    today = datetime.datetime.now()
    d = datetime.timedelta(days = 7)
    before_time = today - d
    
    '''
        Find and get folders to delete
    '''
    to_delete = []
    
    folder_names = ["transit", "latest-replies", "sentiment_topic", "translated_factchecked"]
    
    if "Contents" in folders.keys():
        objects = folders["Contents"]
        for object in objects:
            prefix = object["Key"].split("/")[0]
            if (prefix not in folder_names):
                prefix_date = datetime.datetime.strptime(prefix, '%m%d%Y')
                if (prefix_date <= before_time):
                    to_delete.append(prefix + "/")
                    
    '''
        Delete folders
    '''
    to_delete = set(to_delete)
    s3_resource = boto3.resource('s3')
    for prefix_del in to_delete:
        bucket = s3_resource.Bucket(os.environ["BUCKET_NAME"])
        bucket.objects.filter(Prefix=prefix_del).delete()
            
    return {
        'statusCode': 200,
        'body': json.dumps("Folders older than specified number of days deleted.")
    }
