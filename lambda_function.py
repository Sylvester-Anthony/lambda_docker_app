import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import boto3
from io import StringIO

def lambda_handler(event, context):
    s3_bucket_name = 'apple-stocks-bucket'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_file_key = f'AAPL/AAPL_{timestamp}.csv'
    ticker = yf.Ticker('AAPL')
    end_date = datetime.now()  # Current date
    start_date = end_date - timedelta(days=730)  # 2 years ago
    all_data = []
    current_start_date = start_date
    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=7), end_date)
        try:
            data = ticker.history(start=current_start_date, end=current_end_date, interval='1h')
            all_data.append(data)
        except Exception as e:
            print(f"Error fetching data from {current_start_date} to {current_end_date}: {e}")
        current_start_date = current_end_date
    combined_data = pd.concat(all_data)  
    csv_buffer = StringIO()
    combined_data.to_csv(csv_buffer, index=True)  # Exclude DataFrame index from CSV
    s3 = boto3.client('s3', region_name='us-east-1')
    print('success')
    try:
        s3.put_object(
            Bucket=s3_bucket_name,
            Key=s3_file_key,
            Body=csv_buffer.getvalue()
        )
        print(f"File uploaded successfully to s3://{s3_bucket_name}/{s3_file_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")