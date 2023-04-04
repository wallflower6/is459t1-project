from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import pandas as pd

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def load_data(self, object_link_tweets, object_link_sent_topic):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            session.execute_write(self._load_data, object_link_tweets, object_link_sent_topic)

    @staticmethod
    def _load_data(tx, object_link_tweets, object_link_sent_topic):
        # Write transactions allow the driver to handle retries and transient errors
        query = (f"CALL apoc.load.json('{object_link_tweets}') YIELD value as value_ UNWIND value_.tweets AS tweet "
        "MERGE (p:Post {id:tweet.id}) "
        "ON CREATE SET p.likes=tweet['public_metrics.like_count'], p.text=tweet.clean_text "
        "MERGE (u:User {id:tweet.author_id}) "
        "ON CREATE SET u.name='ZelenskyyUa' "
        "MERGE (u)-[r:POSTED]->(p) "
        "ON CREATE SET r.created_at=apoc.date.fromISO8601(tweet.created_at)")
        result = tx.run(query)

        query = (f"CALL apoc.load.json('{object_link_tweets}') yield value unwind value.replies as reply "
        "MERGE (p:Post {id:toString(reply.id)}) "
        "ON CREATE SET p.text=reply.clean_text, p.likes=reply.favorite_count "
        "MERGE (u:User {id:toString(reply['user.id'])}) "
        "ON CREATE SET u.name=reply['user.screen_name'], u.followers_num=reply['user.followers_count'], u.created_at=apoc.date.parse(reply['user.created_at'],'ms','EEE MMM dd HH:mm:ss Z yyyy') "
        "MERGE (u)-[r:POSTED]->(p) "
        "ON CREATE SET r.created_at=apoc.date.parse(reply.created_at,'ms','EEE MMM dd HH:mm:ss Z yyyy') "
        "WITH reply, p "
        "MERGE (z:Post {id:reply.in_reply_to_status_id_str}) "
        "MERGE (p)-[rt:REPLY_TO]->(z)")
        result = tx.run(query)

        query = (f"CALL apoc.load.json('{object_link_sent_topic}') YIELD value as tweet "
        "UNWIND tweet.topics as topic "
        "MERGE (t:Topic {topic: topic}) "
        "MERGE (p:Post {id:tweet.id_str}) "
        "MERGE (p)-[:BELONGS_TO]->(t)")
        result = tx.run(query)

        query = (f"CALL apoc.load.json('{object_link_sent_topic}') YIELD value as tweet "
        "MERGE (s:Sentiment {sentiment:tweet.sentiment.Sentiment}) "
        "MERGE (p:Post {id:tweet.id_str}) "
        "MERGE (p)-[:BELONGS_TO]->(s)")
        result = tx.run(query)


def lambda_handler(event, context):
    #retrieve event
    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_object = event["Records"][0]["s3"]["object"]["key"]
    
    object_link_sent_topic = f'https://{s3_bucket}.s3.amazonaws.com/{s3_object}'
    object_link_tweets = f'https://{s3_bucket}.s3.amazonaws.com/translated_factchecked/translated-{s3_object.split("/")[-1]}'
    
    uri = "neo4j+s://1ca92074.databases.neo4j.io"
    user = "neo4j"
    password = "Wk7kftUgayhn_WFTLOcsrPsV3HXf4_nEpSMkGjxy24M"
    app = App(uri, user, password)
    # load csv
    app.load_data(object_link_tweets, object_link_sent_topic)
    app.close()