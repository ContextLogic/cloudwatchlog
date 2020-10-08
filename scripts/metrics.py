from prometheus_client import Counter, start_http_server


class Metrics(object):
    def __init__(self, metrics_port):
        start_http_server(metrics_port)
        self.cloudwatchlog = Counter(
            'cloudwatchlog_status', 
            'count of cloudwatch log metrics by status.',
            ['status']
        )


    def cloudwatchlog_successed(self):
        self.cloudwatchlog.labels(status='succcess').inc()


    def cloudwatchlog_failed(self):
        self.cloudwatchlog.labels(status='failure').inc()
