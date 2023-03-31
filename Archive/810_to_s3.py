from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import mysql.connector
import pandas as pd
import io
import boto3
import os
from dotenv import load_dotenv

load_dotenv('.env')

access_key = os.environ['AWS_KEY']
secret_access_key = os.environ['AWS_SECRET']
s3_bucket = os.environ['BUCKET']

cnxn=mysql.connector.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    passwd=os.environ['DB_PWD'],
    database=os.environ['DB_NAME']
)

mysql = create_engine(os.environ['ENG'])

# extract data from mysql server
def extract():
    try:
        engine = mysql
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()
        # execute query
        src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'sslvpn' and table_name in ('tblVSAs','tblTransactions') """)
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))


# load data to s3
def load(df, tbl):
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + str(tbl) + f"/{str(tbl)}"
        filepath =  upload_file_key + ".csv"
        #
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(df)
            print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))


try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
