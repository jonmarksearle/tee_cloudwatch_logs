import sys
import os
import argparse 
import time

def main(argv):

    parser = argparse.ArgumentParser(argument_default=None)
    parser.add_argument('group_name', help='name of the clouldwatch group to write into')
    parser.add_argument('--stream_name', help='name of the cloudwatch stream to write to')
    parser.add_argument('--stream_day' , action='store_true', help='cloudwatch stream name has the day (YYY-MM-DD)')
    parser.add_argument('--stream_hour', action='store_true', help='cloudwatch stream name has the hour (YYY-MM-DD-HH)')
    parser.add_argument('--stream_sec' , action='store_true', help='cloudwatch stream name has the second (YYY-MM-DD-HH-MI-SS)(default)')
    parser.add_argument('--stream_none', action='store_true', help='no datetime addition to the cloudwatch stream name')
    args = parser.parse_args()

    if args.group_name and len(args.group_name)>0 :
        log_group_name = args.group_name
    else:
        parser.print_help()
        quit()

    if args.stream_name and len(args.stream_name)>0 :
        log_stream_name = args.stream_name
    else:
        log_stream_name = log_group_name

    if args.stream_none:
        log_stream_name = log_stream_name
    if args.stream_sec:
        log_stream_name = log_stream_name + '-' + time.strftime("%Y-%m-%d_%H_%M_%S")
    elif args.stream_hour:
        log_stream_name = log_stream_name + '-' + time.strftime("%Y-%m-%d_%H")
    elif args.stream_day:
        log_stream_name = log_stream_name + '-' + time.strftime("%Y-%m-%d")
    else:
        log_stream_name = log_stream_name + '-' + time.strftime("%Y-%m-%d_%H_%M_%S")

    print('Writing to log group',log_group_name,'stream',log_stream_name)

    cwlog = CloudWatchLogsWriter(log_group_name, log_stream_name, "ap-southeast-2")
    try:
        while sys.stdin.readable():
            buff = sys.stdin.readline()
            if not buff:
                break
            buff = buff.strip()
            if len(buff) > 0:
                print (buff)
                cwlog.put_message(buff)
    except (EOFError, SystemExit, KeyboardInterrupt, GeneratorExit):
        sys.stdout.flush()
        pass


import boto3

class CloudWatchLogsWriter():
    ## cut from https://github.com/ImmobilienScout24/python-cloudwatchlogs-logging

    def __init__(self, log_group_name, log_stream_name, region):
        self.region = region
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.connection = None
        self.sequence_token = None

    def _find_last_stream_token(self, log_group_name, log_stream_name):
        print('finding last token for:',log_stream_name)
        response = self.connection.describe_log_streams(
             logGroupName=log_group_name, 
             logStreamNamePrefix=log_stream_name,
             orderBy='LogStreamName',
             descending=False,
             limit=1
        )
        #print('response:',response)
        last_stream_name = response["logStreams"][0]["logStreamName"]
        last_token = response["logStreams"][0]["uploadSequenceToken"]
        print('found stream:',last_stream_name)
        print('found token:',last_token)
        return(last_token)

    def create_group_and_stream(self, log_group_name, log_stream_name):
        try:
            self.connection.create_log_group(logGroupName=log_group_name)
        except self.connection.exceptions.ResourceAlreadyExistsException:
            # Log group already exists so continue as normal
            pass
        try:
            self.connection.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        except self.connection.exceptions.ResourceAlreadyExistsException:
            # Log stream already exists so find the last token and continue
            self.sequence_token = self._find_last_stream_token(log_group_name, log_stream_name)
            pass

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

