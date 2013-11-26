try:
    import json
except ImportError:
    import simplejson as json

import boto
from boto.connection import AWSQueryConnection
from boto.regioninfo import RegionInfo
from boto.exception import JSONResponseError


class KinesisConnection(AWSQueryConnection):
    """
    AWS Kinesis
    """
    APIVersion = "2013-11-04"
    DefaultRegionName = "us-east-1"
    DefaultRegionEndpoint = "kinesis.us-east-1.amazonaws.com"
    ServiceName = "Kinesis"
    TargetPrefix = "com.amazonaws.kinesis.v20131104.Kinesis_20131104"
    ResponseError = JSONResponseError

    _faults = {
    }


    def __init__(self, **kwargs):
        region = kwargs.pop('region', None)
        if not region:
            region = RegionInfo(self, self.DefaultRegionName,
                                self.DefaultRegionEndpoint)

        if 'host' not in kwargs:
            kwargs['host'] = region.endpoint

        AWSQueryConnection.__init__(self, **kwargs)
        self.region = region

    def _required_auth_capability(self):
        return ['hmac-v4']

    def list_streams(self, start_stream_name=None, stream_limit=None):
        params = {}
        if start_stream_name is not None:
            params['ExclusiveStartStreamName'] = start_stream_name
        if stream_limit is not None:
            params['Limit'] = stream_limit
        return self.make_request(action='ListStreams', body=json.dumps(params))

    def create_stream(self, stream_name, shards):
        params = {
            'ShardCount': shards,
            'StreamName': stream_name,
        }
        return self.make_request(action='CreateStream', body=json.dumps(params))

    def delete_stream(self, stream_name):
        params = {
            'StreamName': stream_name,
        }
        return self.make_request(action='DeleteStream', body=json.dumps(params))

    def describe_stream(self, stream_name, start_shard_id=None, shard_limit=None):
        params = {
            'StreamName': stream_name,
        }
        if start_shard_id is not None:
            params['ExclusiveStartShardId'] = start_shard_id
        if shard_limit is not None:
            params['Limit'] = shard_limit
        return self.make_request(action='DescribeStream', body=json.dumps(params))

    def get_shard_iterator(self, stream_name, shard_id, iterator_type, start_seq_num=None):
        params = {
            'StreamName': stream_name,
            'ShardId': shard_id,
            'ShardIteratorType': iterator_type,
        }
        if start_seq_num is not None:
            params['StartingSequenceNumber'] = start_seq_num
        return self.make_request(action='GetShardIterator', body=json.dumps(params))

    def get_next_records(self, shard_iterator, record_limit=None):
        params = {
            'ShardIterator': shard_iterator
        }
        if record_limit is not None:
            params['Limit'] = record_limit
        return self.make_request(action='GetNextRecords', body=json.dumps(params))

    def put_record(self, stream_name, partition_key, data, hash_key=None, min_seq_num=None):
        params = {
            'StreamName': stream_name,
            'PartitionKey': partition_key,
            'Data': data,
        }
        if hash_key is not None:
            params['ExplicitHashKey'] = hash_key
        if min_seq_num is not None:
            params['ExplicitMinimumSequenceNumber'] = min_seq_num
        return self.make_request(action='PutRecord', body=json.dumps(params))

    def merge_shards(self, stream_name, shard_id, adjacent_shard_id):
        params = {
            'StreamName': stream_name,
            'ShardToMerge': shard_id,
            'AdjacentShardToMerge': adjacent_shard_id,
        }
        return self.make_request(action='MergeShards', body=json.dumps(params))

    def split_shard(self, stream_name, shard_id, start_hash_key):
        params = {
            'StreamName': stream_name,
            'ShardToSplit': shard_id,
            'NewStartingHashKey': start_hash_key,
        }
        return self.make_request(action='SplitShard', body=json.dumps(params))

    def make_request(self, action, body):
        headers = {
            'X-Amz-Target': '%s.%s' % (self.TargetPrefix, action),
            'Host': self.region.endpoint,
            'Content-Type': 'application/x-amz-json-1.1',
            'Content-Length': str(len(body)),
        }
        http_request = self.build_base_http_request(
            method='POST', path='/', auth_path='/', params={},
            headers=headers, data=body)
        response = self._mexe(http_request, sender=None,
                              override_num_retries=10)
        response_body = response.read()
        boto.log.debug(response_body)
        if response.status == 200:
            if response_body:
                return json.loads(response_body)
        else:
            json_body = json.loads(response_body)
            fault_name = json_body.get('__type', None)
            exception_class = self._faults.get(fault_name, self.ResponseError)
            raise exception_class(response.status, response.reason,
                                  body=json_body)

