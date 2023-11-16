from datetime import datetime, timedelta
import boto3
import pandas as pd
from io import StringIO
from custom_modules.logs import log
import logging
import requests
import json


def time_checker():
    try:
        current_dt = datetime.now()
        current_dt_str = datetime.strftime(current_dt, '%Y-%m-%d')
        x = current_dt_str.split("-")
        if current_dt > datetime(int(x[0]), int(x[1]), int(x[2]), 13, 0, 0):
            return "Send Slack alert"
    except Exception as e:
        log(
            f"An error occured while checking whether it's past processing time, error: {e}", logging.ERROR, logging.error)


def send_slack_alert(slack_url):
    try:
        log(
            "Sending Slack alert", logging.INFO, logging.info)

        data = json.dumps(
            {"text": f"Transactions from today not yet in, {datetime.now()}"})
        request = requests.post(slack_url, data=data)
        if request.status_code == 200:
            log(
                "Slack alert sent", logging.INFO, logging.info)
        else:
            log(
                f"Slack alert not sent, {request.text}", logging.ERROR, logging.error)
    except Exception as e:
        log(
            f"An error occured while sending Slack alert, error: {e}", logging.ERROR, logging.error)


def retrieve_file(bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str, slack_url: str):
    try:
        log(
            "Retrieving file from s3", logging.INFO, logging.info)

        # calc yesterday's date
        yesterday_dt = datetime.now() - timedelta(1)
        yesterday_dt = datetime.strftime(yesterday_dt, '%Y-%m-%d')

        # retrieve file
        client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

        object_key = f'transactions-{yesterday_dt}.csv'
        csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
        csv_string = csv_obj['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(csv_string))
        return df
    except Exception as e:
        log(
            f"An error occured while retrieve file from s3, error: {e}", logging.ERROR, logging.error)

        if "(NoSuchKey)" in str(e):
            return_value = time_checker()
            if return_value == "Send Slack alert":
                send_slack_alert(slack_url)

            return "File has not yet arrived"
        else:
            return None
