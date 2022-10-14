import boto3
import os
import sys
import string
import random
import logging
import time
import json
import urllib
from threading import Thread
import tempfile
from pathlib import Path


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

log_fmt_long = logging.Formatter(
    fmt='%(asctime)s %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

log_handler_stream = logging.StreamHandler(sys.stdout)
log_handler_stream.setFormatter(log_fmt_long)
log_handler_stream.setLevel(logging.INFO)
logger.addHandler(log_handler_stream)


BUCKET_NAME = 'test-bucket-' + ''.join(random.choice(string.ascii_lowercase) for i in range(10))
QUEUE_NAME = 's3-bucket-monitor-' + ''.join(random.choice(string.ascii_lowercase) for i in range(10))

client_options = {
    'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
    'region_name': os.environ.get('AWS_REGION', 'us-east-1'),
}

s3_client = boto3.client('s3', **client_options)
sqs_client = boto3.client('sqs', **client_options)


class BucketUploaderThread(Thread):
    
    def __init__(self):
        super().__init__()
        self._should_exit: bool = False

    @property
    def should_exit(self):
        return self._should_exit
    
    @should_exit.setter
    def should_exit(self, value: bool):
        self._should_exit = value

    def run(self):
        counter = 0
        with tempfile.TemporaryDirectory() as tempdir:
            while True:
                if self.should_exit:
                    return

                our_file = Path(tempdir, f'test_{counter}')
                counter += 1

                our_file.write_text('ignore me')

                try:
                    logger.info(f'Uploading {our_file.name}')
                    s3_client.upload_file(str(our_file), BUCKET_NAME, our_file.name)
                except Exception:
                    logger.exception(f'Could not upload {our_file.name}')
    
                # three seconds between uploads
                time.sleep(3)

bucket_uploader_thread = BucketUploaderThread()


def cleanup():
    # stop thread
    logger.info('Stopping thread')
    bucket_uploader_thread.should_exit = True

    time.sleep(4)

    # delete bucket and everything in it
    try:
        logger.info(f'Deleting bucket {BUCKET_NAME}')

        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME)

        for page in page_iterator:
            if 'Contents' in page:
                for content in page['Contents']:
                    key = content['Key']
                    s3_client.delete_object(Bucket=BUCKET_NAME, Key=content['Key'])
        s3_client.delete_bucket(Bucket=BUCKET_NAME)
    except Exception:
        logger.exception('Could not delete bucket')

    # delete queue
    if 'queue_url' in globals():
        logger.info(f'Deleting queue {QUEUE_NAME}')
        try:
            sqs_client.delete_queue(QueueUrl=globals()['queue_url'])
        except Exception:
            logger.exception('Could not delete queue')

    sys.exit(0)

# create a bucket
try:
    logger.info(f'Creating bucket {BUCKET_NAME}')
    if client_options['region_name'] == "us-east-1":
        s3_client.create_bucket(Bucket=BUCKET_NAME)
    else:
        location = {"LocationConstraint": client_options['region_name']}
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration=location,
        )
except Exception:
    logger.exception('Could not create bucket')
    cleanup()

# create a queue
try:
    logger.info(f'Creating queue {QUEUE_NAME}')
    queue_url = sqs_client.create_queue(QueueName=QUEUE_NAME)['QueueUrl']
except Exception:
    logger.exception('Could not create queue')
    cleanup()

# sleep 1 second to make sure the queue is available
time.sleep(1)

# get queue ARN
try:
    logger.info(f'Getting queue arn for {QUEUE_NAME}')
    queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
except Exception:
    logger.exception('Could not get queue arn')
    cleanup()

# set queue policy
sqs_policy = {
    "Version": "2012-10-17",
    "Id": "example-ID",
    "Statement": [
        {
            "Sid": "Monitor-SQS-ID",
            "Effect": "Allow",
            "Principal": {
                "AWS":"*"  
            },
            "Action": [
                "SQS:SendMessage"
            ],
            "Resource": queue_arn,
            "Condition": {
                "ArnLike": {
                    "aws:SourceArn": f"arn:aws:s3:*:*:{BUCKET_NAME}"
                },
            }
        }
    ]
}

try:
    logger.info('Setting queue attributes')
    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            'Policy': json.dumps(sqs_policy),
        }
    )
except exception as e:
    logger.exception('could not set queue attributes')
    cleanup()

# configure the bucket notification system
try:
    logger.info(f'Setting bucket notification configuration')
    bucket_notification_config = {
        'QueueConfigurations': [
            {
                'QueueArn': queue_arn,
                'Events': [
                    's3:ObjectCreated:*',
                ]
            }
        ],
    }
    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration=bucket_notification_config
    )
except exception as e:
    logger.exception('could not set bucket notification configuration')
    cleanup()

# start a thread that starts uploading files to the bucket
bucket_uploader_thread.start()

# start a while loop to monitor the queue for new messages. Kill it with Ctrl-C
try:
    logger.info('Infinite while loop starting. Press Ctrl-C to terminate')

    while True:
        try:
            resp = sqs_client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10,
            )
        except Exception as e:
            cleanup()

        if 'Messages' not in resp:
            logger.info('No messages found')
            continue

        for message in resp['Messages']:
            body = json.loads(message['Body'])
            # we are going to assume 1 record per message
            try:
                record = body['Records'][0]
                event_name = record['eventName']
            except Exception as e:
                logger.info(f'Ignoring {message=} because of {str(e)}')
                continue

            if event_name.startswith('ObjectCreated'):
                # new file created!
                s3_info = record['s3']
                object_info = s3_info['object']
                key = urllib.parse.unquote_plus(object_info['key'])
                logger.info(f'Found new object {key}')

        # delete messages from the queue
        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        try:
            resp = sqs_client.delete_message_batch(
                QueueUrl=queue_url, Entries=entries
            )
        except Exception as e:
            cleanup()

        if len(resp['Successful']) != len(entries):
            logger.warn(f"Failed to delete messages: entries={entries!r} resp={resp!r}")

except KeyboardInterrupt:
    logger.info('Ctrl-C caught!')
    cleanup()
