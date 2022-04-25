import boto3
from config import ACCESS_KEY_ID, ACCESS_SECRET_KEY, SNS_TOPIC_ARN, SQS_URL


sns = boto3.client(
     "sns",
     region_name="eu-west-3",
     aws_access_key_id=ACCESS_KEY_ID,
     aws_secret_access_key=ACCESS_SECRET_KEY
)

sqs = boto3.client(
     "sqs",
     region_name="eu-west-3",
     aws_access_key_id=ACCESS_KEY_ID,
     aws_secret_access_key=ACCESS_SECRET_KEY
)

lmbd = boto3.client(
      "lambda",
      region_name="eu-west-3",
      aws_access_key_id=ACCESS_KEY_ID,
      aws_secret_access_key=ACCESS_SECRET_KEY
)

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key= ACCESS_SECRET_KEY, 
    region_name = 'eu-west-3',
)


def sqs_to_sns():
    '''Function to check messages in SQS, publish it in SNS and delete from SQS'''
    try:
        print('Cheking SQS for messages.')
        sqs_messages = sqs.receive_message(QueueUrl=SQS_URL)
        if 'Messages' in sqs_messages.keys():
            msg_count = 0
            for msg in sqs_messages['Messages']:
                sns.publish(TopicArn=SNS_TOPIC_ARN, Message=msg['Body'])
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=msg['ReceiptHandle'])
                msg_count += 1
            return print(f'{msg_count} message(s) pulled from SQS and sent to SNS.')
        else:
            return print('No messages to pull. Next check in 2 minutes.')
    except Exception as e:
        print(e)


def invoke_lambda():
    response = lmbd.invoke(
        FunctionName='Task9-uploads-batch-notifier',
        InvocationType='RequestResponse')
    return response
