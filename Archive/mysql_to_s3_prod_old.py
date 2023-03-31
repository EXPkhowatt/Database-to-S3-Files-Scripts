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
mysql_acm2 = create_engine(os.environ['ENG_acm2'])

mysql_aux1 = create_engine(os.environ['ENG_aux1'])
mysql_aux2 = create_engine(os.environ['ENG_aux2'])

mysql_810 = create_engine(os.environ['ENG_810'])
mysql_810_2 = create_engine(os.environ['ENG_810_2'])

time_now = datetime.now().time()

# extract data from mysql server
def extract():

    logging.info('Starting get SMC database tables first pass')

    #tables from SMC database first pass
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('customer','bb_instance','bb_device','bb_history','circuit','circuit_status','circuit_protocol_type','circuit_transport_type','location','sales_account_manager','sf_guard_user_profile','asset','asset_ip_info','billed_lineitem_status','billed_status','contract_type','delivery','delivery_budget','delivery_contact','delivery_lineitem_status','delivery_status','order_type','phase','product','product_category','project_complexity','quote','quote_group_market_impact','quote_location','region','sf_guard_user','user_productivity','delivery_report_lineitem_status','asset_alias','asset_audit','asset_break_fix_vendor','asset_disposal_reason','asset_ip_type','asset_managed_by','asset_manufacturer','asset_model','asset_os_type','asset_patch_engineer','asset_patch_night','asset_pod','asset_status','asset_type','app','auto_watch','auto_watch_asset','auto_watch_category','auto_watch_problem','auto_watch_service','auto_watch_user','business_service','business_service_product','cabinet','cadence_log','category','change_asset','change_control','change_customer','change_status','change_type','customer_business_service','customer_region','customer_resource_link','customer_user','customer_type','department','facility','incident','incident_asset','incident_customer','incident_facility','incident_impact','incident_status') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get SMC database tables first pass')
    logging.info('Starting get SMC database tables second pass')

    #tables from SMC database second pass
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name = 'delivery_lineitem' """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select *, GetTotalImpactForLineitem(lineitem_id, quantity) as net_impact FROM {tbl[0]}', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name = 'lineitem' """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select *, GetTotalImpactForLineitem(id, quantity) as net_impact FROM {tbl[0]}', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get SMC database tables second pass')
    logging.info('Started get SMC database tables third pass')

    #tables from SMC database third pass
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('ticket','note') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE created_at > date_sub(sysdate(),interval 1 day)', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_insert(df, tbl[0])

            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('note','ticket') """)
            for tbl in src_tables:
               # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]} WHERE updated_at > date_sub(sysdate(),interval 1 day)', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc_update(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get SMC database tables third pass')
    logging.info('Starting get billing-service account table data')

    #tables from billing-service database
    if time_now >= time(4) and time_now < time(9):
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
                load_billing_service(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    logging.info('Completed get billing-service account table data')
    logging.info('Starting get 810 tables data')

    #tables from 810 server
    #sslvpn tables
    if time_now >= time(4) and time_now < time(9):
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
                load_sslvpn(df, tbl[0])
        except Exception as e:
            print("Data extract error on 810: " + str(e))
    else:
        pass

    logging.info('Completed get tblVSAs table data')
    logging.info('Starting get tblTransactions table data')

    #tblTransactions
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

    logging.info('Completed get tblTransactions table data')
    logging.info('Starting get vcoloburst table data')

    #vcoloburst tables
    if time_now >= time(4) and time_now < time(9):
        try:
            engine = mysql_810_2
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'vcoloburst' and table_name in ('vcoloburst') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
                load_vcoloburst(df, tbl[0])
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
        except Exception as e:
            print("Data extract error on 810: " + str(e))
    else:
        pass

    logging.info('Completed get vcoloburst table data')
    logging.info('Starting get vcoloburst poll_data table data')

    #vcoloburst poll_data table
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
    logging.info('Starting get vcenter_new data')

    #vcenter_new data
    if time_now >= time(4) and time_now <= time(9):
        try:
            engine = mysql_aux2
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'vcenter-new' and table_name in ('resource_pool','virtual_center') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
                load_vcenter_new(df, tbl[0])
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
        except Exception as e:
            print("Data extract error on aux1: " + str(e))
    else:
        pass

    logging.info('Completed get vcenter_new table data')
    logging.info('Starting get power_utilizations table data')

    #power_utilizations
    if time_now >= time(10) and time_now <= time(11):
        try:
            engine = mysql_aux1
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'power-etl' and table_name in ('power_utilizations') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
                load_power_etl(df, tbl[0])
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
        except Exception as e:
            print("Data extract error on aux1: " + str(e))
    else:
        pass

    logging.info('Completed get power_utilizations table data')

# load data to s3
def load_smc(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'smc/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
        filepath =  upload_file_key + ".csv"
        # write to s3
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False, escapechar='\\')

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

    logging.info('Completed write smc tables and push to S3')

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
            df.to_csv(csv_buffer, index=False, escapechar='\\')

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
            df.to_csv(csv_buffer, index=False, escapechar='\\')

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

def load_billing_service(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'billing_service/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write billing_service tables and push to S3')

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

def load_vcenter_new(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'vcenter_new/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write vcenter_new tables and push to S3')

def load_power_etl(df, tbl):
    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'power_etl/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

    logging.info('Completed write power_etl tables and push to S3')

logging.warning('Start procedure to get MySQL table data and write to S3 csv files')

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

logging.info('Completed ETL process to push MySQL data to S3')
