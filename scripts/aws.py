import boto3
import time
import datetime
import logging


class CloudWatchLog(object):

    def __init__(self, group_name, region):
        self.group_name = group_name
        self.client = boto3.client('logs', region_name=region)
        self.time = datetime.datetime.utcnow()
        self.stream_name = self.time.strftime("%Y-%m-%d")
        self.token = ""


    # update stream name as %Y-%m-%d
    def update_stream_name(self):
        cur_time = datetime.datetime.utcnow()
        cur_stream_name = cur_time.strftime("%Y-%m-%d")
        if cur_stream_name != self.stream_name:
            self.stream_name = cur_stream_name
            self.token = ""


    # get the token. Create log stream if not exist.
    def prepare_send(self):
        log_streams = self.client.describe_log_streams(logGroupName=self.group_name, logStreamNamePrefix=self.stream_name)['logStreams']
        for log_stream in log_streams:
            if self.stream_name == log_stream['logStreamName']:
                if 'uploadSequenceToken' in log_stream:
                    self.token = log_stream['uploadSequenceToken']
                    return
                else:
                    # first time to write to a log stream
                    self.token = ""
        # don't find stream_name in cloudwatchlog
        self.client.create_log_stream(logGroupName=self.group_name, logStreamName=self.stream_name)


    # send data to cloudwatch log
    def send_data(self, data, metrics):
        self.update_stream_name()

        if not self.token:
            self.prepare_send()

        response = ""
        if not self.token:
            timestamp = int(round(time.time() * 1000))
            try:
                response = self.client.put_log_events(
                    logGroupName=self.group_name,
                    logStreamName=self.stream_name,
                    logEvents=[
                        {
                            'timestamp': timestamp,
                            'message': data
                        }
                    ],
                )
            except Exception as e:
                logging.exception('Fail to send log to aws cloudwatch by put_log_events() method. '+ str(e))
                return False
        else:
            timestamp = int(round(time.time() * 1000))
            try:
                response = self.client.put_log_events(
                    logGroupName=self.group_name,
                    logStreamName=self.stream_name,
                    logEvents=[
                        {
                            'timestamp': timestamp,
                            'message': data
                        }
                    ],
                    sequenceToken=self.token
                )
            except Exception as e:
                logging.exception('Fail to send log to aws cloudwatch by put_log_events() method. '+ str(e))
                return False
        
        if 'nextSequenceToken' in response:
            self.token = response['nextSequenceToken']
            return True
        else:
            self.token = ''
            logging.error("No nextSequenceToken from aws cloudwatch response.")
            return False