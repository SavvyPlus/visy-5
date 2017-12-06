import boto3

from datetime import datetime, time
s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch', region_name='ap-southeast-2')
sns = boto3.client('sns')

total_cleared_file_prefix = 'SITHE_DISPATCHIS_'

input_bucket = 'visy-5-input'
processing_bucket = 'visy-5-processing'
processed_bucket = 'visy-5-processed'
failed_bucket = 'visy-5-failed'

total_topic= 'arn:aws:sns:ap-southeast-2:547051082101:visy-5-min-total-cleared-alerts'
alert_start_time = time(8,0,0)
alert_end_time = time(18,0,0)

def create_s3_key(dt=datetime.now()):
    """
    Based on the file prefix and date, create an s3 key to fetch the correct file
    """
    date_str = dt.strftime('%Y/%m/%d/%H/%M')
    print('%s/%s' % (total_cleared_file_prefix, date_str))


def move_file(src_bucket, src_key, dst_bucket, dst_key):
    s3.copy({'Bucket': src_bucket, 'Key': src_key}, dst_bucket, dst_key)
    s3.delete_object(Bucket=src_bucket, Key=src_key)


def publish_total_cleared_delta(total_cleared, initial_mw, dt):

    delta = total_cleared - initial_mw

    cloudwatch.put_metric_data(
        Namespace='visy',
        MetricData=[
            {
                'MetricName':'totalcleared_delta',
                'Dimensions': [
                    {
                        'Name':'value',
                        'Value': str(delta)
                    }
                ],
                'Timestamp': dt,
                'Value': delta
            }
        ]
    )


def should_publish_alert(now = datetime.now().time()):
    """
    Returns True if an alert should be published to an SNS topic
    """
    return alert_start_time <= now <= alert_end_time


def publish_alert(dt, total_cleared, initial_value):
    """
    Publish alert to SNS topic
    """
    sns.publish(
        TopicArn = total_topic,
        Message = 'TotalCleared [%s] does not equal InitialMW [%s] at [%s]' % (total_cleared, initial_value, dt)
    )


def process_file(filename):
    try:

        move_file("foamdino-test", "%s/%s" % (input_bucket, filename), "foamdino-test", "%s/%s" % (processing_bucket, filename))

        obj = s3.get_object(Bucket="foamdino-test", Key="%s/%s" %(processing_bucket, filename))

        data = obj['Body'].read().decode('utf-8')
        #C,NEMP.WORLD,DISPATCHIS,AEMO,SITHE,2017/10/26,00:25:11,0000000288575627,DISPATCHIS,0000000288575626
        #I,DISPATCH,UNIT_SOLUTION,2,SETTLEMENTDATE,RUNNO,DUID,TRADETYPE,DISPATCHINTERVAL,INTERVENTION,CONNECTIONPOINTID,DISPATCHMODE,AGCSTATUS,INITIALMW,TOTALCLEARED,RAMPDOWNRATE,RAMPUPRATE,LOWER5MIN,LOWER60SEC,LOWER6SEC,RAISE5MIN,RAISE60SEC,RAISE6SEC,DOWNEPF,UPEPF,MARGINAL5MINVALUE,MARGINAL60SECVALUE,MARGINAL6SECVALUE,MARGINALVALUE,VIOLATION5MINDEGREE,VIOLATION60SECDEGREE,VIOLATION6SECDEGREE,VIOLATIONDEGREE,LASTCHANGED,LOWERREG,RAISEREG,AVAILABILITY,RAISE6SECFLAGS,RAISE60SECFLAGS,RAISE5MINFLAGS,RAISEREGFLAGS,LOWER6SECFLAGS,LOWER60SECFLAGS,LOWER5MINFLAGS,LOWERREGFLAGS,RAISEREGAVAILABILITY,RAISEREGENABLEMENTMAX,RAISEREGENABLEMENTMIN,LOWERREGAVAILABILITY,LOWERREGENABLEMENTMAX,LOWERREGENABLEMENTMIN,RAISE6SECACTUALAVAILABILITY,RAISE60SECACTUALAVAILABILITY,RAISE5MINACTUALAVAILABILITY,RAISEREGACTUALAVAILABILITY,LOWER6SECACTUALAVAILABILITY,LOWER60SECACTUALAVAILABILITY,LOWER5MINACTUALAVAILABILITY,LOWERREGACTUALAVAILABILITY,SEMIDISPATCHCAP
        #D,DISPATCH,UNIT_SOLUTION,2,"2017/10/26 00:30:00",1,SITHE01,0,20171025246,0,NSYW1,0,0,0,0,180,60,0,0,0,0,0,0,,,,,,,,,,,"2017/10/26 00:25:05",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
        #I,DISPATCH,OFFERTRK,1,SETTLEMENTDATE,DUID,BIDTYPE,BIDSETTLEMENTDATE,BIDOFFERDATE,LASTCHANGED
        #D,DISPATCH,OFFERTRK,1,"2017/10/26 00:30:00",SITHE01,ENERGY,"2017/08/01 00:00:00","2017/08/01 02:22:18","2017/10/26 00:25:05"
        #C,"END OF REPORT",6

        #initial_mw = None
        #total_cleared = None
        lines = data.splitlines()
        for l in lines:
            if l.startswith('D,DISPATCH,UNIT_SOLUTION'):
                fields = l.split(",")
                date_str = fields[4].replace('"', '')
                #this is the correct code, but sample files
                #dt = datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')
                dt = datetime.now()

                initial_mw = int(fields[13])
                total_cleared = int(fields[14])

        if initial_mw >=0 and total_cleared >=0:
            publish_total_cleared_delta(total_cleared, initial_mw, dt)
        else:
            print(initial_mw)
            print(total_cleared)
            print("unable to parse initial_mw or total_cleared for %s" % (filename,))
            move_file("foamdino-test", "%s/%s" % (processing_bucket, filename), "foamdino-test", "%s/%s" % (failed_bucket, filename))
            return

    except Exception as e:
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

    process_file("test-total-cleared-file")

def lambda_handler(event, context):
    # call create_s3_key with a value
    in_date = "2017/10/26 20:40:00"
    dt = datetime.strptime(in_date, '%Y/%m/%d %H:%M:%S')
    create_s3_key(dt)

    print("Object added to: [%s]" % (event['Records'][0]['s3']['bucket']['name'],))
    filename = event['Records'][0]['s3']['object']['key'].split('/')[-1]
    print(filename)

    #process_file("test-file")
    process_file(filename)
