from aws_cdk import (
    core,
    aws_dynamodb as ddb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as les,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
)


class MediaTranscriptionStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Tag all constructs with the project for easy billing drilldown,
        # filtering, and organization.
        core.Tags.of(self).add('project', 'MediaTranscription')

        # Media files bucket
        media_bucket = s3.Bucket(
            self,
            'media-transcription-bucket',
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # SQS queue for media files bucket event notifications
        media_bucket_event_queue = sqs.Queue(
            self,
            'media-transcription-event-notification-queue',
            queue_name='media-transcription-event-notification-queue',
            visibility_timeout=core.Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=sqs.Queue(
                    self,
                    'media-transcription-event-notifications-dlq',
                    queue_name='media-transcription-event-notifications-dlq',
                )
            ),
        )

        # S3 object created notifications sent to SQS queue
        media_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(media_bucket_event_queue),
            *[s3.NotificationKeyFilter(prefix='media-input/')],
        )

        # Lambda function to create/submit Transcribe jobs
        transcribe_job_init_fn = lambda_.Function(
            self,
            'transcribe-job-init-fn',
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler='fn.handler',
            code=lambda_.Code.from_asset('../lambdas/transcribe-job-init-fn'),
            reserved_concurrent_executions=1,  # Effectively single-threaded
        )
        # Triggered by SQS messages created for media file puts
        transcribe_job_init_fn.add_event_source(
            les.SqsEventSource(
                queue=media_bucket_event_queue,
                batch_size=5,
                enabled=True,
            )
        )
        transcribe_job_init_fn.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    'transcribe:StartTranscriptionJob',
                ],
                resources=['*'],
                effect=iam.Effect.ALLOW,
            )
        )

        # DynamoDB table for Jobs metadata
        jobs_metadata_table = ddb.Table(
            self,
            'MediaTranscription-TranscriptionJobs',
            table_name='MediaTranscription-TranscriptionJobs',
            partition_key=ddb.Attribute(
                name='Bucket-Key-ETag',
                type=ddb.AttributeType.STRING,
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
        )
        jobs_metadata_table.grant(
            transcribe_job_init_fn.grant_principal,
            *[
                'dynamodb:GetItem',
                'dynamodb:PutItem',
            ]
        )
