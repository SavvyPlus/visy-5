import boto3


from datetime import datetime
s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch', region_name='ap-southeast-2')


conformance_file_prefix = 'SITHE_DISPATCH_CONFORMANCE_'

input_bucket = 'visy-5-input'
processing_bucket = 'visy-5-processing'
processed_bucket = 'visy-5-processed'
failed_bucket = 'visy-5-failed'

def create_s3_key(dt=datetime.now()):
    """
    Based on the file prefix and date, create an s3 key to fetch the correct file
    If given a datetime will use that to create the key, else it will create a key based on now
    """
    date_str = dt.strftime('%Y/%m/%d/%H/%M')
    print("%s%s" % (conformance_file_prefix,date_str))


def move_file(src_bucket, src_key, dst_bucket, dst_key):
    """
    Moves a file from src to dst
    """
    print("moving file")
    s3.copy({'Bucket': src_bucket, 'Key': src_key}, dst_bucket, dst_key)
    s3.delete_object(Bucket=src_bucket, Key=src_key)


def publish_conformance_data(conformance_val, conformance_dt, conformance, message):
    """
    publish val to cloudwatch
    """
    cloudwatch.put_metric_data(
        Namespace='visy',
        MetricData=[
            {
                'MetricName':'conformance',
                'Dimensions': [
                    {
                        'Name': 'value',
                        'Value': conformance_val,
                        'Name': 'message',
                        'Value': message
                    }
                ],
                'Timestamp':conformance_dt,
                'Value':conformance_val
            }
        ]
    )


def process_file(filename):
    # move file from input to processing
    try:
        move_file("foamdino-test", "%s/%s" % (input_bucket, filename), "foamdino-test", "%s/%s" % (processing_bucket, filename))

        # get file and parse data
        obj = s3.get_object(Bucket="foamdino-test", Key="%s/%s" % (processing_bucket, filename))
        #print(obj)
        data = obj['Body'].read().decode('utf-8')
        #C,NEMP.WORLD,DISPATCH_CONFORMANCE,AEMO,SITHE,2017/10/26,00:30:10,0000000288575738,DISPATCH_CONFORMANCE,0000000288575738
        #I,DISPATCH,UNIT_CONFORMANCE,1,INTERVAL_DATETIME,DUID,TOTALCLEARED,ACTUALMW,ROC,AVAILABILITY,RAISEREG,LOWERREG,STRIGLM,LTRIGLM,MWERROR,MAX_MWERROR,LECOUNT,SECOUNT,STATUS,PARTICIPANT_STATUS_ACTION,OPERATING_MODE,LASTCHANGED
        #D,DISPATCH,UNIT_CONFORMANCE,1,"2017/10/26 00:30:00",SITHE01,0,0,1,0,0,0,6,8,0,0,0,0,NORMAL,"No action required. Unit is following dispatch target",AUTO,"2017/10/26 00:30:09"
        #C,"END OF REPORT",4
        #
        #print(data)

        lines = data.splitlines()
        for l in lines:
            if l.startswith('D'):
                fields = l.split(',')
                conformance = fields[18]
                message = fields[19]
                conformance_date_str = fields[4].replace('"', '')
                #conformance_date_str must be in the last two weeks - test with datetime.now, but this is the correct code
                #conformance_dt = dt = datetime.strptime(conformance_date_str, '%Y/%m/%d %H:%M:%S')
                conformance_dt = datetime.now()
                if 'NORMAL' == conformance:
                    conformance_val = 0
                else:
                    conformance_val = 1

        publish_conformance_data(conformance_val, conformance_dt, conformance, message)


    except Exception as e:
        # move file to failed
        print(e)
        print("moving to failed bucket for checking %s" % (filename,))
        move_file("foamdino-test", "%s/%s" % (processing_bucket, filename), "foamdino-test", "%s/%s" % (failed_bucket, filename))
        return

    print("moving to processed bucket %s" % (filename,))
    move_file("foamdino-test", "%s/%s" % (processing_bucket, filename), "foamdino-test", "%s/%s" % (processed_bucket, filename))


# main method for testing outside of lambda environment
if __name__ == '__main__':
    # call create_s3_key with no value
    create_s3_key()

    # call create_s3_key with a value
    in_date = "2017/10/26 20:40:00"
    dt = datetime.strptime(in_date, '%Y/%m/%d %H:%M:%S')
    create_s3_key(dt)

    process_file("test-file")
