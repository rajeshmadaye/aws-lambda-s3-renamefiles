#************************************************************************
## Lambda Function  : RenameFilesFunction
## Description      : Lambda Function take file from bucket-1 and rename and upload to bucket-02
## Author           :
## Copyright        : Copyright 2021
## Version          : 1.0.0
## Mmaintainer      :
## Email            :
## Status           : In Review
##************************************************************************
## Version Info:
## 1.0.0 : 10-Sep-2021 : Created first version to rename file to S3
##************************************************************************
import sys, os, json
from urllib.parse import unquote_plus
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import traceback

##************************************************************************
## Global Default Parameters :: User should modify default values.
##************************************************************************
DEF_SOURCE_BUCKET_NAME          = 'bucket-01'
DEF_DEST_BUCKET_NAME            = 'bucket-02'
DEF_REPLACE_SOURCE_PATTERN      = '0_1_2_3_4.csv'   # ABC_test_123_20190912_0010.csv
DEF_REPLACE_DEST_PATTERN        = '0_1_4_2_3.csv'   # ABC_test_0010_123_20190912.csv
DEF_FILENAME_DELI               = '_'

##***********************************************************************
## Class Definition
##***********************************************************************
class ReplaceFileNames:
  #************************************************************************
  # Class constructor
  #************************************************************************
  def __init__(self, key):
    rc = True
    self.sourceBucket   = self.get_env_value('SOURCE_BUCKET_NAME', DEF_SOURCE_BUCKET_NAME)
    self.destBucket     = self.get_env_value('DEST_BUCKET_NAME', DEF_DEST_BUCKET_NAME)
    self.sourceString   = self.get_env_value('REPLACE_SOURCE_PATTERN', DEF_REPLACE_SOURCE_PATTERN)
    self.destString     = self.get_env_value('REPLACE_DEST_PATTERN', DEF_REPLACE_DEST_PATTERN)
    self.filenameDeli   = self.get_env_value('FILENAME_DELI', DEF_FILENAME_DELI)
    self.inputfile      = key
    self.s3_resource    = boto3.resource('s3')
    self.s3_client      = boto3.client('s3')
    return

  #************************************************************************
  # Setup environment parameters if exists
  #************************************************************************
  def get_env_value(self, key, default):
    value = os.environ[key] if key in os.environ else default
    return value

  #************************************************************************
  # Function to run main logic
  #************************************************************************
  def run(self):
    rc = False
    if self.isValidBucketName():
      self.prepareSourcePatternMap()
      self.prepareDestPatternMap()
      self.mapPattern()
      # self.replaceAllFilenames()
      if self.inputfile:
        self.replaceFilename()
      rc = True
    return rc

  #************************************************************************
  # Validate if bucket name is correct.
  #************************************************************************
  def isValidBucketName(self):
    rc = True

    if(rc and not self.s3_resource.Bucket(self.sourceBucket) in self.s3_resource.buckets.all()):
      print("Error:: Unable to find bucket name : {}".format(self.sourceBucket))
      rc = False

    if(rc and not self.s3_resource.Bucket(self.sourceBucket) in self.s3_resource.buckets.all()):
      print("Error:: Unable to find bucket name : {}".format(self.sourceBucket))
      rc = False

    return rc

  #************************************************************************
  # Prepare map
  #************************************************************************
  def prepareSourcePatternMap(self):
    rc = True
    self.sourcePattern = self.preparePatternMap(self.sourceString)
    return rc

  def prepareDestPatternMap(self):
    rc = True
    self.destPattern = self.preparePatternMap(self.destString)
    return rc

  def preparePatternMap(self, str):
    rc = True
    map = {}
    if str:
      string = str.split('.', -1)
      pattern = string[0].split(self.filenameDeli, -1)
      map = { 'pattern' : pattern, 'ext' : string[-1] }
    else:
      print("Error :: Failed to process, string pattern not defined", str)

    return map

  def mapPattern(self):
    rc = True
    map = {}
    for num in (range(0, len(self.sourcePattern['pattern']))):
      map[num] = self.destPattern['pattern'][num]
    self.map = map
    return map

  #************************************************************************
  # Prepare output file name
  #************************************************************************
  def getOutFilename(self, inpfile):
    rc = True
    outfile = None
    string = inpfile.split('.', -1)
    name = string[0].split(self.filenameDeli, -1)
    for num in (range(0, len(self.sourcePattern['pattern']))):
      mapIndex = int(self.map[num])
      if outfile:
        outfile = outfile + self.filenameDeli + name[mapIndex]
      else:
        outfile = name[mapIndex]
      num += 1
    outfile = outfile + '.' + self.destPattern['ext']
    return outfile

  #************************************************************************
  # Replace all files names
  #************************************************************************
  def replaceAllFilenames(self):
    rc = True
    bucket = self.s3_resource.Bucket(self.sourceBucket)
    for obj in bucket.objects.all():
      key = obj.key
      try:
        inpfile = obj.key
        download_path = '/tmp/{}'.format(inpfile)
        outfile = inpfile.replace('.csv','.txt')
        outfile = self.getOutFilename(inpfile)
        print("File: [{}/{}] => [{}/{}]".format(self.sourceBucket, inpfile, self.destBucket, outfile))
        upload_path = '/tmp/{}'.format(outfile)
        self.s3_client.download_file(self.sourceBucket, inpfile, download_path)
        os.rename(download_path,upload_path)
        self.s3_client.upload_file(upload_path, self.destBucket, outfile)
        if os.path.exists(download_path):
          os.remove(download_path)
        if os.path.exists(upload_path):
          os.remove(upload_path)
        self.s3_client.delete_object(Bucket=self.sourceBucket, Key=inpfile)
      except Exception as e:
        print("Exception in rename file")
        traceback.print_exc()

    return rc

  #************************************************************************
  # Replace all files names
  #************************************************************************
  def replaceFilename(self):
    rc = True
    bucket = self.s3_resource.Bucket(self.sourceBucket)
    inpfile = self.inputfile
    try:
      download_path = '/tmp/{}'.format(inpfile)
      outfile = self.getOutFilename(inpfile)
      print("File: [{}/{}] => [{}/{}]".format(self.sourceBucket, inpfile, self.destBucket, outfile))
      upload_path = '/tmp/{}'.format(outfile)
      self.s3_client.download_file(self.sourceBucket, inpfile, download_path)
      os.rename(download_path,upload_path)
      self.s3_client.upload_file(upload_path, self.destBucket, outfile)
      if os.path.exists(download_path):
        os.remove(download_path)
      if os.path.exists(upload_path):
        os.remove(upload_path)
      self.s3_client.delete_object(Bucket=self.sourceBucket, Key=inpfile)
    except Exception as e:
      print("Exception in rename file:", inpfile)
      traceback.print_exc()
    return rc

#************************************************************************
# main lambda handler
#************************************************************************
def lambda_handler(event, context):
  rc = False
  print("INFO :: Lambda function executon initiated")
  for record in event['Records']:
    # bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    try:
      RFN = ReplaceFileNames(key)
      rc = RFN.run()
      print("Lambda Execution Status :", rc)
    except Exception as inst:
      print("Error:: Unable to process request:", inst)
      traceback.print_exc()

    print("INFO :: Lambda function executon completed")

  return rc

if __name__ == "__main__":
    lambda_handler('','')
