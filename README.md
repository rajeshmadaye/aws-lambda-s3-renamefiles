# Lambda function to rename files as per requested pattern and load to another S3 bucket.
This project contains source code and supporting files for a python based Amazon lambda function to read files 
from bucket-1 and read requested pattern and rename file names accordingly and load to another S3 bucket.

## Features
- Lambda function to read all the files from bucket-1
- read requested pattern (file rename logic as per business requirement)
- Rename file names and load to bucket-2


## Tech
Below are list of technologies used.
- [Python] - Python based lambda function.
- [boto3] - Python boto3 SDK used to interact with AWS services.

Below are list of AWS services used in this project.
- [S3]     - Boto3 client object used to interact with AWS S2 instances.
- [Lambda]  - AWS Lambda function created.


## Package installation steps

User should use below command to create this package.
```bash
sam package --region $AWSRegion --profile $ProfileName --s3-bucket $BucketName --template-file $BuiltTemplate --output-template-file deploy.yaml
```

User should use below command to deploy this package.
```bash
sam deploy --region $AWSRegion --profile $ProfileName --s3-bucket $BucketName --template-file $BuiltTemplate --stack-name $StackName --capabilities CAPABILITY_IAM

```


## License
MIT

