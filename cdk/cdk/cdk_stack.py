from aws_cdk import (
    core,
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
