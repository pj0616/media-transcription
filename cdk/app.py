#!/usr/bin/env python3

from aws_cdk import core

from cdk.cdk_stack import MediaTranscriptionStack


app = core.App()
MediaTranscriptionStack(app, 'MediaTranscriptionStack-v100')

app.synth()
