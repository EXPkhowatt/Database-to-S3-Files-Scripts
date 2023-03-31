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

mysql_aux1 = create_engine(os.environ['ENG_aux3'])
mysql_aux2 = create_engine(os.environ['ENG_aux4'])

time_now = datetime.now().time()
date = (str(date.today()) + " " + "04:40:00")

# extract data from mysql server
def extract():

    #tables from Main-1
    #tables from SMC database insert/update
    logging.info('Start get SMC database tables insert/update')

    #SMC tables note
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('note') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= date_sub(current_date, interval day(current_date) day) and created_at <= concat(curdate()," 05:00:00")', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_insert(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('note') """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE updated_at >= date_sub(sysdate(),interval 3 day) and updated_at <= concat(curdate()," 05:00:00") and created_at >= "2021-01-01 00:00:00"', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_update(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('note') """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select distinct id FROM {tbl[0]} WHERE created_at >= "2021-01-01 00:00:00"', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_dstct_note(df, tbl[0])    

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
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= date_sub(current_date, interval day(current_date) day) and created_at <= concat(curdate()," 05:00:00")', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_insert(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('log') """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE updated_at >= date_sub(sysdate(),interval 3 day) and updated_at <= concat(curdate()," 05:00:00") and created_at >= date_sub(sysdate(),interval 7 month)', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_update(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get SMC database tables insert/update')

    #SMC ticket table
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('ticket') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select id, status_id, source_id, visibility_id, severity_id, queue_id, customer_id, category_id, problem_id, problem_template_id, template_id, subject, address, hold_end_at, time_held, ttr, ttc, is_escalated, is_phone, created_by, updated_by,assigned_to, reopened_by, held_by, resolved_by, closed_by, created_at, updated_at, internal_updated_at, update_due_at, close_due_at, assigned_at, moved_at, reopened_at, held_at, resolved_at, closed_at, vantive_id, ticket_scheduler_id, ticket_template_id, legacy_id, legacy_system_id, is_survey_accessed, close_out_at, is_awaiting_response, acknowledgement, acknowledged_at, is_master_outage, escalate_for_review_start_at, is_lync, hold_reason_id, sub_problem_id, helpful_yes_count, helpful_no_count, helpful_neutral_count, root_cause_id, root_cause_other, parent_ticket_scheduler_id, max_severity_id, metadata FROM {tbl[0]} WHERE created_at >= date_sub(current_date, interval day(current_date) day) and created_at <= concat(curdate()," 05:00:00")', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_insert(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('ticket') """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select id, status_id, source_id, visibility_id, severity_id, queue_id, customer_id, category_id, problem_id, problem_template_id, template_id, subject, address, hold_end_at, time_held, ttr, ttc, is_escalated, is_phone, created_by, updated_by,assigned_to, reopened_by, held_by, resolved_by, closed_by, created_at, updated_at, internal_updated_at, update_due_at, close_due_at, assigned_at, moved_at, reopened_at, held_at, resolved_at, closed_at, vantive_id, ticket_scheduler_id, ticket_template_id, legacy_id, legacy_system_id, is_survey_accessed, close_out_at, is_awaiting_response, acknowledgement, acknowledged_at, is_master_outage, escalate_for_review_start_at, is_lync, hold_reason_id, sub_problem_id, helpful_yes_count, helpful_no_count, helpful_neutral_count, root_cause_id, root_cause_other, parent_ticket_scheduler_id, max_severity_id, metadata FROM {tbl[0]} WHERE updated_at >= date_sub(sysdate(),interval 3 day) and updated_at <= concat(curdate()," 05:00:00") and created_at >= "2021-01-01 00:00:00"', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_update(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass


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
                df = pd.read_sql_query(f'select * FROM {tbl[0]} where from_unixtime(unixdate) >= date_sub(current_date, interval day(current_date) day) and from_unixtime(unixdate) <= concat(curdate()," 05:00:00")', engine)
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
                df = pd.read_sql_query(f'select * FROM {tbl[0]} where poll_timestamp >= date_sub(current_date, interval day(current_date) day) and poll_timestamp <= concat(curdate()," 04:40:00")', engine)
                load_vcoloburst(df, tbl[0])
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
        except Exception as e:
            print("Data extract error on 810: " + str(e))
    else:
        pass

    logging.info('Completed get vcoloburst poll_data table data')


    #tables from AUX
    #tables from call_metrics database insert
    logging.info('Start get call_metrics database tables insert')

    #call_metrics tables call_csq_agent
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_aux1
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'call_metrics' and table_name in ('call_csq_agent') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= date_sub(current_date, interval day(current_date) day) and created_at <= concat(curdate()," 05:00:00")', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_call_metrics_insert(df, tbl[0])

        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get call_metrics call_csq_agent table data')

    #tables from shift_report database insert
    #shift_report tables walk_result
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_aux2
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'shift_report' and table_name in ('walk_result') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at >= date_sub(current_date, interval day(current_date) day) and created_at <= concat(curdate()," 05:00:00")', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_shift_report_insert(df, tbl[0])

        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get call_metrics call_csq_agent table data')

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

def load_dstct_note(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'smc/' + str(tbl) + f"/distinct/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

def load_call_metrics_insert(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'call_metrics/' + str(tbl) + f"/insert/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write call_metrics table inserts and push to S3')

def load_shift_report_insert(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'shift_report/' + str(tbl) + f"/insert/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write shift_report table inserts and push to S3')

logging.warning('Start procedure to get MySQL table data and write to S3 csv files')

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

logging.info('Completed ETL process to push MySQL data to S3')