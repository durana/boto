from boto.regioninfo import RegionInfo


def regions():
    """
    Get all available regions for the AWS Kinesis service.

    :rtype: list
    :return: A list of :class:`boto.regioninfo.RegionInfo`
    """
    from boto.kinesis.layer1 import KinesisConnection

    return [RegionInfo(name='us-east-1',
                       endpoint='kinesis.us-east-1.amazonaws.com',
                       connection_cls=KinesisConnection),
            ]


def connect_to_region(region_name, **kw_params):
    for region in regions():
        if region.name == region_name:
            return region.connect(**kw_params)
    return None