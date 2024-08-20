import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import boto3
from io import StringIO

def lambda_handler(event, context):
    # AWS S3 configuration
    s3_bucket_name = 'apple-stocks-bucket'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_file_key = f'AAPL/AAPL_{timestamp}.csv'

    # Set the ticker symbol
    ticker = yf.Ticker('AAPL')

    # Define the date range
    end_date = datetime.now()  # Current date
    start_date = end_date - timedelta(days=730)  # 2 years ago

    # Initialize a list to store the data
    all_data = []

    # Iterate over the date range in 7-day increments
    current_start_date = start_date

    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=7), end_date)
        # Fetch data for the current 7-day period
        try:
            data = ticker.history(start=current_start_date, end=current_end_date, interval='1h')
            all_data.append(data)
        except Exception as e:
            print(f"Error fetching data from {current_start_date} to {current_end_date}: {e}")
        
        # Move to the next interval
        current_start_date = current_end_date

    # Concatenate all data into a single DataFrame
    combined_data = pd.concat(all_data)

    # Convert DataFrame to CSV using an in-memory buffer
    csv_buffer = StringIO()
    combined_data.to_csv(csv_buffer, index=True)  # Exclude DataFrame index from CSV

    # Initialize S3 client
    s3 = boto3.client('s3', region_name='us-east-1')
    print('success')
    
    # Upload the CSV to the S3 bucket
    try:
        s3.put_object(
            Bucket=s3_bucket_name,
            Key=s3_file_key,
            Body=csv_buffer.getvalue()
        )
        print(f"File uploaded successfully to s3://{s3_bucket_name}/{s3_file_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")