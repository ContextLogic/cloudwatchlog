FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python-pip

RUN pip install boto3 prometheus_client

WORKDIR /cloudwatchlog

COPY scripts ./scripts

ENV PYTHONPATH /cloudwatchlog/scripts:$PYTHONPATH

CMD [ "python", "./scripts/main.py" ]
