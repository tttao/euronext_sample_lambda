import json
import boto3
import time
from botocore.exceptions import ClientError

def get_client(service, endpoint=None, region="us-east-1"):
    import botocore.session as bc
    session = bc.get_session()

    s = boto3.Session(botocore_session=session, region_name=region)
    if endpoint:
        return s.client(service, endpoint_url=endpoint)
    return s.client(service)
    
def sql_status(query_id):
    res = boto3.client("redshift-data").describe_statement(Id=query_id)
    status = res["Status"]
    if status == "FAILED":
        raise Exception('Error:' + res["Error"])
    return status.strip('"')
    
def publish_cloudwatch(log_string):
    cwl = get_client('logs')
    logGroupName="/euronext/redshift"
    logStreamName="stream_from_lambda"
    events = []
    events.append({"timestamp": int(time.time()*1000), "message": log_string})
    
    uploadSequenceToken = ""
    try:
        cwl.put_log_events(
            logGroupName=logGroupName,
            logStreamName=logStreamName,
            logEvents=[
                {
                    "timestamp": int(time.time()*1000),
                    "message": "fake data"
                }
            ],
            sequenceToken=str(int(time.time()*1000))
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidSequenceTokenException":
            if "expectedSequenceToken" in e.response:
                uploadSequenceToken = e.response["expectedSequenceToken"]
            elif "The next expected sequenceToken is: null" in e.response["Error"]["Message"]:
                uploadSequenceToken = None

    # Build args to be sent to put call
    putArgs = {
        'logGroupName': logGroupName,
        'logStreamName': logStreamName,
        'logEvents': events
    }
    if uploadSequenceToken is not None:
        putArgs['sequenceToken'] = uploadSequenceToken
    
    cwl.put_log_events(**putArgs)
        
def handle_results(records):
    
    for record in records:
        record_columns = []
        for column in record:
            assert len(column) == 1
            for k, v in column.items():
                if k == "isNull":
                    assert v == True
                    record_columns.append("null")
                else:
                    record_columns.append(str(v))
        log_string = ",".join(record_columns)
        publish_cloudwatch(log_string)
        

def lambda_handler(event, context):

    rsd = get_client('redshift-data')
    
    #
    # Execute query
    #
    res = rsd.execute_statement(
        SecretArn="arn:aws:secretsmanager:us-east-1:794137955756:secret:euronext/sample/redshift-b9qsA2",
        ClusterIdentifier="redshift-cluster-1",
        Database="dev",
        Sql='select "table", unsorted,vacuum_sort_benefit from svv_table_info order by 1;')
    
    #
    # Wait for query execution
    #
    query_id = res["Id"]
    statuses = ["FAILED", "FINISHED"]
    done = False
    while not done:
        status = sql_status(query_id)
        if status in statuses:
            print(query_id + ":" + status)
            break
    #   
    # Parse result
    #
    res = rsd.get_statement_result(Id=query_id)
    while res.get("NextToken"):
        print_records(res["Records"])
        res = rsd.get_statement_result(
            Id=id, NextToken=res["NextToken"]
        )
    
    #
    # Loop results & publish to cloudwatch
    #
    handle_results(res["Records"])
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
