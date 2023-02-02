import json
import boto3
import uuid
import os

client_control = boto3.client('s3control', region_name='us-east-1')
source_bucket_arn = os.environ.get('sourceBucketArn')
destination_bucket_arn = os.environ.get('targetBucketArn')
manifest_file = os.environ.get('manifestFile')
role_arn = os.environ.get('s3BatchRole')
obj_arn = source_bucket_arn+'/Manifest/'+manifest_file
account_id =  os.environ.get('accountId')
source_bucket_name = os.environ.get('sourceBucket')
#get ETag
key_etag = 'Manifest/'+manifest_file
s3_client = boto3.client('s3')

def lambda_handler(event, context):
  try:
    #print(event)
    response = s3_client.get_object(Bucket=source_bucket_name, Key=key_etag)
    etag = response['ETag']
    clientRequestToken=str(uuid.uuid4())
    print(clientRequestToken)
    operationDetails={
      "S3PutObjectCopy": {
        "TargetResource": source_bucket_arn,
        "MetadataDirective": "COPY",
        "StorageClass": "STANDARD",
        "BucketKeyEnabled": True,
        "TargetKeyPrefix": "Copy"
      }
    }
    reportDetails={
      "Format": "Report_CSV_20230120",
      "Bucket": source_bucket_arn,
      "Enabled": True,
      "ReportScope": "AllTasks",
      "Prefix": "Report"
    }
    manifestDetails={
      "Spec": {
        "Format": "S3BatchOperations_CSV_20230120",
        "Fields":["Bucket","Key"]
      },
      "Location": {
        "ObjectArn": obj_arn,
        "ETag": etag
      }
    }
    response=client_control.create_job(
      AccountId=account_id,
      ConfirmationRequired=False,
      Description='s3 batch ops job triggered by lambda',
      ClientRequestToken=clientRequestToken,
      Priority=1,
      RoleArn=role_arn,
      Operation=operationDetails,
      Report=reportDetails,
      Manifest=manifestDetails
    )
    print(response)
    return {
      "message": "True"
    }
  except Exception as exc:
    return {
      "message": "False",
      "Error": str(exc)
    }