# flask-aws-rest

A simple web-application for AWS EC2 to store images in S3 bucket and metadata of images in RDS (PostgreSQL). 

An application has some endpoints: 
- to show the name of the region and AZ the application is running in;
- to show, delete and upload images to S3 bucket;
- to subscribe and unsubscribe to SNS topic;
- to trigger AWS lambda function which pulled messages from SQS and sent it to SNS.
