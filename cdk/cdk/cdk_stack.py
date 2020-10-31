from aws_cdk import (
    core,
    aws_s3 as s3,
)


class MediaTranscriptionStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Tag all constructs with the project for easy billing drilldown,
        # filtering, and organization.
        core.Tags.of(self).add('project', 'MediaTranscription')

        # Media files bucket
        s3.Bucket(
            self,
            'media-transcription-bucket',
            encryption=s3.BucketEncryption.S3_MANAGED,
        )
