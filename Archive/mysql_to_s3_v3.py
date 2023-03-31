from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import mysql.connector
import pandas as pd
import io
import boto3
import os
from datetime import date, datetime
from dotenv import load_dotenv
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging.info('Starting MySQL files to S3 ETL process')

load_dotenv('.env')

access_key = os.environ['AWS_KEY']
secret_access_key = os.environ['AWS_SECRET']
s3_bucket = os.environ['BUCKET']

mysql_acm = create_engine(os.environ['ENG_acm'])
mysql_acm2 = create_engine(os.environ['ENG_acm2'])

mysql_810 = create_engine(os.environ['ENG_810'])

# extract data from mysql server
def extract():

    logging.info('Starting get SMC asset_ip_info and customer table data')

    #tables from SMC database
    try:
        engine = mysql_acm
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()
        # execute query
        src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('asset','asset_ip_info','customer','bb_instance','bb_device','bb_history') """)
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
            df = df.replace(r'\\','/', regex=True)
            df = df.replace(r'\n',' ', regex=True)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error on acm: " + str(e))

    logging.info('Completed get SMC asset_ip_info and customer table data')
    logging.info('Starting get billing-service account table data')

    #tables from billing-service database
    try:
        engine = mysql_acm2
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()
        # execute query
        src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'billing-service' and table_name = 'account' """)
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
            df = df.replace(r'\\','/', regex=True)
            df = df.replace(r'\n',' ', regex=True)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error on acm: " + str(e))

    logging.info('Completed get billing-service account table data')
    logging.info('Starting get 810 tables data')

    #tables from 810 database
    try:
        engine = mysql_810
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()
        # execute query
        src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'sslvpn' and table_name in ('tblVSAs') """)
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
            df = df.replace(r'\\','/', regex=True)
            df = df.replace(r'\n',' ', regex=True)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error on 810: " + str(e))

    logging.info('Completed get tblVSAs table data')
    logging.info('Starting get tblTransactions table data')

    #tblTransactions
    try:
        engine = mysql_810
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()
        # execute query
        src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'sslvpn' and table_name in ('tblTransactions') """)
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]} where from_unixtime(unixdate) > date_sub(sysdate(), interval 1 day)', engine)
            load(df, tbl[0])
            df = df.replace(r'\\','/', regex=True)
            df = df.replace(r'\n',' ', regex=True)
    except Exception as e:
        print("Data extract error on 810: " + str(e))

    logging.info('Completed get tblTransactions table data')

# load data to s3
def load(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
        filepath =  upload_file_key + ".csv"
        # write to s3
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

    logging.info('Completed write table and push to S3')

logging.warning('Start procedure to get MySQL table data and write to S3 csv files')

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

logging.info('Completed ETL process to push MySQL data to S3')
