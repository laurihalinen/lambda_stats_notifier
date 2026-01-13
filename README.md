# lambda_stats_notifier
Automated cloud infrastructure for uploading and processing CS2 player statistics. When a .csv file is uploaded to S3, a Lambda function parses the data and stores it in DynamoDB. Another Lambda function triggers an SNS notification that emails subscribers with the top 3 players based on win rate.
