from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import mysql.connector
import pandas as pd
import io
import boto3
import os
from datetime import date, datetime, time
from dotenv import load_dotenv
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging.info('Starting MySQL files to S3 ETL process')

load_dotenv('.env')

access_key = os.environ['AWS_KEY']
secret_access_key = os.environ['AWS_SECRET']
s3_bucket = os.environ['BUCKET2']

mysql_acm = create_engine(os.environ['ENG_acm'])
mysql_810 = create_engine(os.environ['ENG_810'])
mysql_810_2 = create_engine(os.environ['ENG_810_2'])

time_now = datetime.now().time()

# extract data from mysql server
def extract():

    #tables from Main-1
    #tables from SMC database insert/update
    logging.info('Start get SMC database tables insert/update')

    #SMC tables note, ticket
    if time_now >= time(4) and time_now < time(17):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('ticket') """)
            
            for tbl in src_tables:
                # query and load save data to dataframe
                #df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= date_sub(sysdate(),interval 1 day)', engine)
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= "2022-01-01 00:00:00" and created_at <= "2023-02-05 05:00:00"', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(to_replace=[r"\r|\n|\t"], value=[""], regex=True)
                df = df.replace(r"\n","", regex=True)
                #df = df.replace(r"\")
                #df = df.replace(r'"',"'",regex=True)
                load_smc_insert(df, tbl[0])

            #src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('note') """)
            #for tbl in src_tables:
               # query and load save data to dataframe
            #    df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE updated_at >= date_sub(sysdate(),interval 1 day) and created_at >= "2021-01-01 00:00:00"', engine)
            #    df = df.replace(r'\\','/', regex=True)
            #    df = df.replace(r'\n',' ', regex=True)
            #    load_smc_update(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    #SMC tables log
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('log') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= date_sub(sysdate(),interval 1 day)', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_insert(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('log') """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE updated_at >= date_sub(sysdate(),interval 1 day) and created_at >= date_sub(sysdate(),interval 6 month)', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_update(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get SMC database tables insert/update')


    #tables from 810 server
    #sslvpn tblTransactions
    logging.info('Starting get tblTransactions insert')

    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_810
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'sslvpn' and table_name in ('tblTransactions') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} where from_unixtime(unixdate) > date_sub(sysdate(), interval 1 day)', engine)
                load_sslvpn(df, tbl[0])
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
        except Exception as e:
            print("Data extract error on 810: " + str(e))
    else:
        pass

    logging.info('Completed get tblTransactions insert')

    #vcoloburst poll_data
    logging.info('Starting get vcoloburst poll_data table data')

    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_810_2
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'vcoloburst' and table_name in ('poll_data') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} where poll_timestamp > date_sub(sysdate(), interval 1 Day)', engine)
                load_vcoloburst(df, tbl[0])
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
        except Exception as e:
            print("Data extract error on 810: " + str(e))
    else:
        pass

    logging.info('Completed get vcoloburst poll_data table data')

# load data to s3
def load_smc_insert(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'smc/' + str(tbl) + f"/insert/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write smc table inserts and push to S3')

def load_smc_update(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'smc/' + str(tbl) + f"/update/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write smc table updates and push to S3')

def load_sslvpn(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'sslvpn/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write sslvpn tables and push to S3')

def load_vcoloburst(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'vcoloburst/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write vcoloburst tables and push to S3')

logging.warning('Start procedure to get MySQL table data and write to S3 csv files')

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

logging.info('Completed ETL process to push MySQL data to S3')