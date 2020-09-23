FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python-pip

RUN pip install boto3

WORKDIR /cloudwatchlog

COPY scripts ./scripts

ENV PYTHONPATH /cloudwatchlog/scripts:$PYTHONPATH

CMD [ "python", "./scripts/ship_log.py" ]
