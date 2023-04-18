# A_Assignment_Sam
Data Engineer Applicant Assessment
Search Engine based Total Revenue
Presentor: Sameera Prasad

Abstract:
This project calculates the revenue for the client getting from external Search Engines and which keywords are performing the best based on revenue

AWS Services like S3, Lambda and Cloudwatch logs are a perfect example to notify the user whenever a file is uploaded, generated and downloaded. 
User will upload the file and the pipeline will generate the Total Revenue for every domain based on its keyword

Steps:
User will upload the “.tsv” format in the folder
Lambda function triggers as soon as a file has been uploaded.
For granting permissions, IAM role has been created and set it as lambda’s execution role
The source and destination of the file has been declared in environment tags for security
Existing Dependencies / customized dependencies can be added to lambda function using a .zip file
As soon as a new file is uploaded on a daily/weekly basis, the lambda will trigger the python script to generate the file in the desired format
User can monitor/view logs via Cloudwatch 
Unit test cases have been added for better testing of the functionality

Test Requirements:
If there are multiple dates in a single file
If no revenue is generated
Keywords such as “IPOD” , “iPod” , “ipod” are considered as a matching keyword.
Observations:
With the help of Pandas and Matplot lib functions, the best keyword generated based on Revenue appears to be ‘Ipod’


Sources:
Github 
AWS S3
AWS Lambda
AWS IAM Role
Unittest

https://docs.google.com/document/d/1qiQltlkE9-iRct83GCeRFO1cnI6m7NaNcvnYg-3ovfg/edit
