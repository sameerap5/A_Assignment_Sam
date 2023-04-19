import pandas as pd
import json
import re
import boto3
from datetime import datetime
from Controller import extract_revenue_data_from_tsv
import os


def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    bucket = os.environ["bucket_name"]
    input_file_name = os.environ["input_file_name"]
    output = s3.get_object(Bucket=bucket, Key=input_file_name)
    data = pd.read_csv(output['Body'], sep='\t')
    result, formatted_date_sets = extract_revenue_data_from_tsv(data)

    # seperate file based on every date
    for formatted_date in formatted_date_sets:
        output_file_name = '{formatted_date}_SearchKeywordPerformance.tsv'.format(formatted_date=formatted_date)
        date_to_filter = pd.to_datetime(formatted_date).strftime('%Y-%m-%d')
        filtered_df = result[result['date'] == date_to_filter]

        # Calculate total revenue generated based on search engine and keyword
        filtered_df = filtered_df.groupby(['Search Engine Domain', 'Search Keyword'])['Revenue'].sum()
        filtered_df = filtered_df.reset_index().sort_values(by=['Revenue'],ascending=False)
        filtered_df = filtered_df.groupby(filtered_df['Search Keyword'])['Revenue'].mean()
        filtered_df.plot(kind='bar')
        plt.title('Mean revenue by Search Keyword')
        plt.xlabel('Search Keyword')
        plt.ylabel('Mean Revenue')
        plt.show()
        # print(filtered_df)
        output_file_name = '{formatted_date}_SearchKeywordPerformance.tsv'.format(formatted_date=formatted_date)
        response = filtered_df.to_csv("/tmp/{output_file_name}.format(output_file_name=output_file_name)", sep = '\t')
        response = s3.upload_file("/tmp/{output_file_name}.format(output_file_name=output_file_name)",bucket,output_file_name )
      
