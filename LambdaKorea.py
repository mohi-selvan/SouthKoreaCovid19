import json
import boto3
import base64

#create glue client
client = boto3.client('glue')
clients = boto3.client('sns')
topic_arn = 'arn:aws:sns:us-west-1:143812385682:DataInsertedSNSAlarm'

def lambda_handler(event, context) :
    try:
       client.start_crawler(Name='225S3Crawler')
    except Exception as e:
        print(e)
        print('Error starting crawler')
        raise e
    try:
        clients.publish(TopicArn=topic_arn, Message='S3DataInsert', Subject='DataInsert')
        print('Successfully delivered alarm message')
    except Exception:
        print('Delivery failure')
    

