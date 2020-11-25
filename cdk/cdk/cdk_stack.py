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
            code=lambda_.Code.from_asset(
                '../lambdas/transcribe-job-init-fn',
                # The following is just dumb.
                # The Lambda runtime doesn't use the latest boto3 by default.
                # In order to use the latest boto3, we have to pip install
                # and bundle locally using Docker.
                # Q: Why need the latest boto3?
                # A: https://github.com/boto/boto3/issues/2630
                # I'll have to delete the ECR containers to avoid cost.
                # TODO: Revert back to normal in like a month I guess.
                bundling={
                    'image': lambda_.Runtime.PYTHON_3_8.bundling_docker_image,
                    'command': [
                        'bash',
                        '-c',
                        '\n        pip install -r requirements.txt -t /asset-output &&\n        cp -au . /asset-output\n        '
                    ]
                }
            ),
            handler='fn.handler',
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
        # Grant access to start transcription jobs
        transcribe_job_init_fn.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    'transcribe:StartTranscriptionJob',
                ],
                resources=['*'],
                effect=iam.Effect.ALLOW,
            )
        )

        # Grant Lambda role to read and write to input and output portions of
        # the S3 bucket.
        # Q: Why grant Lambda the permissions instead of Transcribe service?
        # A: Two-fold:
        #   -  i) https://amzn.to/321Nx5I
        #   - ii) Granting just to this Lambda means other Transcribe jobs
        #         across the account cannot use this bucket (least privilege).
        media_bucket.grant_read(
            identity=transcribe_job_init_fn.grant_principal,
            objects_key_pattern='media-input/*'
        )
        # Cannot specify a prefix for writes as Transcribe will not accept
        # a job unless it has write permission on the whole bucket.
        # Edit: The above statement was when I had to use '*' for writes. But
        #       now, I granted access to that .write_access_check_file.temp
        #       file and it seems to all work now?
        media_bucket.grant_write(
            identity=transcribe_job_init_fn.grant_principal,
            objects_key_pattern='transcribe-output-raw/*'
        )
        # This is just as frustrating to you as it is to me.
        media_bucket.grant_write(
            identity=transcribe_job_init_fn.grant_principal,
            objects_key_pattern='.write_access_check_file.temp'
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

        # Create IAM Group with read/write permissions to S3 bucket
        # TODO: Make this more federated and robust
        console_users_group = iam.Group(self, 'MediaTranscriptionConsoleUsers')
        console_users_group.attach_inline_policy(policy=iam.Policy(
            self,
            'MediaTranscriptionConsoleUserS3Access',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:ListBucket',
                    ],
                    resources=[
                        media_bucket.bucket_arn,
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetObject',
                        's3:PutObject',
                    ],
                    resources=[
                        media_bucket.arn_for_objects('media-input/*'),
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetObject',
                    ],
                    resources=[
                        media_bucket.arn_for_objects('transcribe-output-raw/*'),
                    ],
                ),
            ],
        ))

        # TODO: CloudWatch event for Transcribe job state change (for later)
        # TODO: S3 trigger on transcription-output-raw/ folder to SQS to Lambda
        # TODO: Implement Lambda to format the raw transcription output
        # TODO: SNS Notification when formatted transcription ready
        # TODO: Step Functions for job coordination:
        #       - Create job
        #       - Wait for job
        #       - Format output
        #       - Sent notification
