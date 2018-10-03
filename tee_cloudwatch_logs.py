import sys
import os

def main(argv):
    # usage:
    #   << batch script >> | python to_cloudwatch_logs.py {cloudwatch log group}
    #   tail -f {log file}.log | python to_cloudwatch_logs.py {cloudwatch log group}
    # read in each line from standard output and put to cloudwatch log group 
    # creating a new stream for each run

    if (len(argv) != 2):
        print('Usage : tail -f {log file}.log | python to_cloudwatch_logs.py {cloudwatch log group}')
        exit

    print('Writing to log group '+argv[1])

    cwlog = CloudWatchLogsWriter(argv[1], "ap-southeast-2")

    try:
        while True:
            buff = sys.stdin.readline().strip()
            print (buff)
            if len(buff) > 0:
                cwlog.put_message(buff)
    except KeyboardInterrupt:
        sys.stdout.flush()
        pass


import boto3
import time

class CloudWatchLogsWriter():
    ## cut from https://github.com/ImmobilienScout24/python-cloudwatchlogs-logging

    def __init__(self, log_group_name, region):
        self.region = region
        self.log_group_name = log_group_name
        self.log_stream_name = log_group_name + '-' + time.strftime("%Y-%m-%d-%H_%M_%S")
        self.connection = None
        self.sequence_token = None

    def create_group_and_stream(self, log_group_name, log_stream_name):
        try:
            self.connection.create_log_group(logGroupName=log_group_name)
        except self.connection.exceptions.ResourceAlreadyExistsException as e:
            # Log group already exists so continue as normal
            pass
        self.connection.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)

    def _lazy_connect(self):
        if self.connection:
            return
        self.connection = boto3.client(service_name='logs', region_name=self.region)
        self.create_group_and_stream(self.log_group_name, self.log_stream_name)

    def _put_message(self, message):
        self._lazy_connect()
        timestamp = int(round(time.time() * 1000))
        event = {"message": message, "timestamp": timestamp}
        if self.sequence_token:
            result = self.connection.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=[event],
                sequenceToken=self.sequence_token)
        else:
            result = self.connection.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=[event])
        self.sequence_token = result.get("nextSequenceToken", None)

    def put_message(self, message):
        try:
            self._put_message(message)
        except self.connection.exceptions.DataAlreadyAcceptedException as e:
            if e.status != 400:
                raise
            next_sequence_token = e.body.get("expectedSequenceToken", None)
            if next_sequence_token:
                self.sequence_token = next_sequence_token
                self._put_message(message)
            else:
                raise


if __name__ == "__main__":
    main(sys.argv)

