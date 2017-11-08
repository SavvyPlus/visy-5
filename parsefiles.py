import sys
import os
import boto3

from datetime import datetime

s3 = boto3.resource('s3')
cloudwatch = boto3.client('cloudwatch')

#file prefixes
prefixes=(
    'PUBLIC_DISPATCHIS_',
    'PUBLIC_DISPATCHSCADA_',
    'PUBLIC_P5MIN_',
    'SITHE_DISPATCH_CONFORMANCE_',
    'SITHE_DISPATCHIS_',
    'SITHE_P5MIN_'
)

bucket='foamdino-test'

def upload_original(f, path):
    key = create_s3_key(f)
    print("uploading to s3: %s" % (key,))
    s3.Object(bucket,key).put(Body=open(os.path.join(path,f), 'rb'))


def upload_parsed(f):
    print("uploading parsed files")

def create_s3_key(filename):
    """
    Creates the appropriate s3 key based on the input filename
    PUBLIC_P5MIN_201710261045_20171026104038.CSV
    SITHE_P5MIN_201710261045_20171026104039.CSV
    """
    f = filename.lower()
    fparts = f.split('_')
    ftype = fparts[0] + '_' + fparts[1]
    year = fparts[2][0:4]
    month = fparts[2][4:6]
    day = fparts[2][6:8]
    hour = fparts[2][8:10]
    mins = fparts[2][10:12]

    return "%s/%s/%s/%s/%s/%s" % (ftype,year,month,day,hour,mins)

def parse_file(fname):
    """
    Parses 5 min file to extract data
    I,P5MIN,UNITSOLUTION,2,RUN_DATETIME,INTERVAL_DATETIME,DUID,CONNECTIONPOINTID,TRADETYPE,AGCSTATUS,INITIALMW,TOTALCLEARED,RAMPDOWNRATE,RAMPUPRATE,LOWER5MIN,LOWER60SEC,LOWER6SEC,RAISE5MIN,RAISE60SEC,RAISE6SEC,LOWERREG,RAISEREG,AVAILABILITY,RAISE6SECFLAGS,RAISE60SECFLAGS,RAISE5MINFLAGS,RAISEREGFLAGS,LOWER6SECFLAGS,LOWER60SECFLAGS,LOWER5MINFLAGS,LOWERREGFLAGS,LASTCHANGED,SEMIDISPATCHCAP
    D,P5MIN,UNITSOLUTION,2,"2017/10/26 20:40:00","2017/10/26 20:40:00",SITHE01,NSYW1,,0,0,0,180,60,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"2017/10/26 20:35:27",0
    """
    valid_line_prefixes = ('C', 'I', 'D')
    with open(fname, 'r') as f:
        lines = f.read().splitlines()
        if not lines[0].startswith(valid_line_prefixes):
            print("cannot parse file, doesn't have correct first line %s" % (lines[0],))

        if not lines[1].startswith(valid_line_prefixes):
            print("cannot parse file, doesn't have correct first line %s" % (lines[1],))

        if not lines[2].startswith(valid_line_prefixes):
            print("cannot parse file, doesn't have correct first line %s" % (lines[2],))

        data = lines[2].split(',')
        d = data[5].replace('"',"")
        #print('[' + data[5].replace('"',"") + ']')
        duid = data[6]
        connection_point_id = data[7]

        dt = datetime.strptime(d, '%Y/%m/%d %H:%M:%S')
        print(dt)




if __name__ == '__main__':
    file_dir = sys.argv[1]
    print("loading files from %s" % (file_dir,))

    for f in os.listdir(file_dir):
        if not f.startswith(prefixes):
            print("Don't know how to process %s" % (f,))
            continue # skip this file

        if 'P5MIN' in f: # ignore rest of files for now
            #upload_original(f, file_dir)
            parse_file(os.path.join(file_dir,f))

            #with open(os.path.join(file_dir,file), 'r') as f:
            #    for line in f:
            #        print(line)
