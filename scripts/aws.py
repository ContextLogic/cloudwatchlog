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


    def update_stream_name(self, token):
        cur_time = datetime.datetime.utcnow()
        cur_stream_name = cur_time.strftime("%Y-%m-%d")
        if cur_stream_name != self.stream_name:
            self.stream_name = cur_stream_name
            return ""
        else:
            return token


    def prepare_send(self):
        log_streams = self.client.describe_log_streams(logGroupName=self.group_name, logStreamNamePrefix=self.stream_name)['logStreams']
        token = ""
        for log_stream in log_streams:
            if self.stream_name == log_stream['logStreamName']:
                if 'uploadSequenceToken' in log_stream:
                    token = log_stream['uploadSequenceToken']
                    return token
                else:
                    return token
        self.client.create_log_stream(logGroupName=self.group_name, logStreamName=self.stream_name)
        return token


    def send_data(self, token, data):
        print "send_data",  self.client, self.stream_name, token, data
        response = ""
        if not token:
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
                print response
            except Exception as e:
                logging.error('Fail to send log to aws cloudwatch by put_log_events() method. '+ str(e))
        else:
            timestamp = int(round(time.time() * 1000))
            print timestamp
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
                    sequenceToken=token
                )
                print response
            except Exception as e:
                logging.error('Fail to send log to aws cloudwatch by put_log_events() method. '+ str(e))
        
        if 'nextSequenceToken' in response:
            return response['nextSequenceToken']
        else:
            logging.error("No nextSequenceToken from aws cloudwatch response.")
            return ""