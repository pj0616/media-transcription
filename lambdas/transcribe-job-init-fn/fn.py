import hashlib
import json
import uuid
from datetime import datetime as dt

import boto3


dynamodb = boto3.resource('dynamodb')
transcribe_client = boto3.client('transcribe')


def handler(event, context):
    print('Request: {}'.format(json.dumps(event)))
    print('Found {} records.'.format(len(event['Records'])))

    # Lambda invocations can have a batch of records. Iterate over them all.
    for record in event['Records']:
        print('Handling message with Id=[{}]'.format(record['messageId']))
        sqs_message = json.loads(record['body'])
        handle(sqs_message)
    print('Completed handling messages.')


def handle(message):
    # Despite there being multiple "Records" in the S3 event notification,
    # currently S3 events only produce a single record per event.
    s3_event = message['Records'][0]

    # S3 object event timestamp
    s3_event_timestamp = s3_event['eventTime']

    # Extract bucket and object info
    s3_details = s3_event['s3']
    object_details = s3_details['object']
    job_details = {
        'bucket_name': s3_details['bucket']['name'],
        'object_key': object_details['key'],
        'object_etag': object_details['eTag'],
        'object_size': object_details['size'],
        's3_event_timestamp': s3_event_timestamp,
    }

    # Check if input object has already been transcribed
    # TODO: Hmm, maybe only check by eTag? This way we de-dupe by contents.
    print('Checking if job request is a duplicate.')
    if is_duplicate(job_details):
        error_msg = (
            'Detected a duplicate job, cause of which could be '
            'duplicated message, multiple puts to same key, or something else.'
        )
        print(error_msg)
        return

    # Start Transcribe job
    print('Starting the Transcribe job.')
    job_id = transcribe_file(
        job_details['bucket_name'],
        job_details['object_key']
    )
    job_details['job_id'] = job_id

    # TODO: Update jobs table
    print('Updating table with job details')
    save_job_metadata(job_details)


def transcribe_file(bucket_name, input_key):
    # Define job parameters
    input_object_uri = create_s3_uri(bucket_name, input_key)
    job_id = str(uuid.uuid4())
    output_key = 'transcribe-output-raw/{}'.format(compute_sha256(input_key))
    print('Using JobId: [{}], Output URI: [{}]'.format(
        job_id,
        create_s3_uri(bucket_name, output_key)
    ))

    response = transcribe_client.start_transcription_job(
        TranscriptionJobName=job_id,
        Media={
            'MediaFileUri': input_object_uri,
        },
        LanguageCode='en-US',
        OutputBucketName=bucket_name,
        OutputKey=output_key,
    )
    print('Transcribe response: {}'.format(response))

    return response['TranscriptionJob']['TranscriptionJobName']


def create_s3_uri(bucket_name, key):
    return 's3://{bucket}/{key}'.format(
        bucket=bucket_name,
        key=key
    )


def compute_sha256(input_key):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_key.encode('utf-8'))
    return sha256_hash.hexdigest()


def is_duplicate(job_details):
    table = dynamodb.Table('MediaTranscription-TranscriptionJobs')
    key = make_table_key(job_details)
    ddb_response = table.get_item(
        Key={
            'Bucket-Key-ETag': key
        }
    )
    return 'Item' in ddb_response


def make_table_key(job_details):
    return '{bucket}-{key}-{etag}'.format(
        bucket=job_details['bucket_name'],
        key=job_details['object_key'],
        etag=job_details['object_etag']
    )


def create_table_item(job_details):
    now = dt.now().isoformat()
    return {
        'Bucket-Key-ETag': make_table_key(job_details),
        'BucketName': job_details['bucket_name'],
        'InputObjectKey': job_details['object_key'],
        'InputObjectETag': job_details['object_etag'],
        'InputObjectSize': job_details['object_size'],
        'S3EventTimestamp': job_details['s3_event_timestamp'],
        'TranscribeJobId': job_details['job_id'],
        'ItemCreatedTimestamp': now,
        'LastUpdatedTimestamp': now,
        'TranscribeJobStatus': 'QUEUED'  # TODO: Do not hard-code
    }


def save_job_metadata(job_details):
    item = create_table_item(job_details)
    table = dynamodb.Table('MediaTranscription-TranscriptionJobs')
    ddb_response = table.put_item(
        Item=item
    )
