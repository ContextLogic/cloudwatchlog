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
        self.create_log_group()


    def create_log_group(self):
        """
            create log group.
        """
        try:
            log_groups = self.client.describe_log_groups(logGroupNamePrefix=self.group_name)["logGroups"]
            for log_group in log_groups:
                if self.group_name == log_group['logGroupName']:
                    return
            # don't find log_name in cloudwatchlog, create one.
            self.client.create_log_group(logGroupName=self.group_name)
        except Exception as e:
            logging.exception('Fail to create aws cloudwatch log group: '+ str(e))


    def update_stream_name(self):
        """
            update stream name as %Y-%m-%d
        """
        cur_time = datetime.datetime.utcnow()
        cur_stream_name = cur_time.strftime("%Y-%m-%d")
        if cur_stream_name != self.stream_name:
            self.stream_name = cur_stream_name
            self.token = ""


    def prepare_send(self):
        """
            get the token. Create log stream if not exist.
        """
        try:
            log_streams = self.client.describe_log_streams(logGroupName=self.group_name, logStreamNamePrefix=self.stream_name)['logStreams']
            for log_stream in log_streams:
                if self.stream_name == log_stream['logStreamName']:
                    self.token = log_stream['uploadSequenceToken']
                    return
            # don't find stream_name in cloudwatchlog, create one
            self.client.create_log_stream(logGroupName=self.group_name, logStreamName=self.stream_name)
        except Exception as e:
            logging.exception('Exception when preparing to send to aws cloudwatch log stream: '+ str(e))


    def send_data(self, data, metrics):
        """
            send data to cloudwatch log
        """
        self.update_stream_name()

        if not self.token:
            self.prepare_send()

        response = ""
        # send data to a newly created log stream does not require a sequence token. 
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