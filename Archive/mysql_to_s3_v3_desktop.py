from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import mysql.connector
import pandas as pd
import io
import boto3
import os
from datetime import date, datetime, time
from dotenv import load_dotenv

#import logging

#logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
#logging.info('Starting MySQL files to S3 ETL process')

load_dotenv('.env')

access_key = os.environ['AWS_KEY']
secret_access_key = os.environ['AWS_SECRET']
s3_bucket = os.environ['BUCKET2']

mysql_acm = create_engine(os.environ['ENG_acm'])
mysql_acm2 = create_engine(os.environ['ENG_acm2'])

mysql_aux1 = create_engine(os.environ['ENG_aux1'])

mysql_810 = create_engine(os.environ['ENG_810'])

time_now = datetime.now().time()

# extract data from mysql server
def extract():

    #logging.inf('Starting get SMC asset_ip_info and customer table data')
    #tables from SMC database
    if time_now >= time(13) and time_now < time(17):
        try:
            engine = mysql_acm
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            # execute query
            src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' """)
            #src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('bb_history') """)
            for tbl in src_tables:
                # query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load(df, tbl[0])
        except Exception as e:
            print("Data extract error on acm: " + str(e))
    else:
        pass

    #asset table from SMC
    #try:
    #    engine = mysql_acm
    #    Session = scoped_session(sessionmaker(bind=engine))
    #    s = Session()
        # execute query
    #    src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'smc' and table_name in ('asset') """)
    #    for tbl in src_tables:
         # query and load save data to dataframe
    #        df = pd.read_sql_query(f""" select id, name, managed_by_id, model_id, asset_type_id, serial_tag, asset_tag, break_fix_vendor_id, break_fix_tag, maintenance_po, maintenance_contract_details,
    #            maintenance_contract_end_date, maintenance_contract_id, maintenance_vendor_po, maintenance_vendor_sales_order, maintenance_notes, service_address_diff_flag,
    #            service_address, service_city, service_state, service_zip, os_type_id, os_version, windows_sp, scom_setup, patched, patch_engineer_id, patch_night_id,
    #            patch_window, patch_notes, case when notes like '%%sub-folders/files:%%' then concat(notes,';') else notes end as notes,
    #            created_at, created_by, updated_at, updated_by, revision, region_id, purchase_po, starcare_node_id, legacy_id, legacy_system_id,
    #            location_id, customer_id, cabinet_id, is_virtual, pod_id, external_id, status_id, inventory_location, asset_classification_id, cbr, parent_asset_id, disposal_reason_id,
    #            asset_backup_information_id, contract_type, pcr, mrr, cost, support_by_expedient, rack, monitoring_id, os_architecture_id, os_version_id, windows_sp_id,
    #            os_distro_id, is_critical, maintenance_interval, communication_id, install_date, warranty_start, warranty_end, capacity, life, condition_index, photo,
    #            maintenance_interval_id, obm_management, obm_console, account_id FROM {tbl[0]}""", engine)
    #        load(df, tbl[0])
    #except Exception as e:
    #    print("Data extract error on Asset Table: " + str(e))

    #logging.info('Completed get SMC asset_ip_info and customer table data')
    #logging.info('Starting get billing-service account table data')

    #tables from billing-service database
    #try:
    #    engine = mysql_acm2
    #    Session = scoped_session(sessionmaker(bind=engine))
    #    s = Session()
        # execute query
    #    src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'billing-service' and table_name = 'account' """)
    #    for tbl in src_tables:
            # query and load save data to dataframe
    #        df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
    #        df = df.replace(r'\\','/', regex=True)
    #        df = df.replace(r'\n',' ', regex=True)
    #        load(df, tbl[0])
    #except Exception as e:
    #    print("Data extract error on acm: " + str(e))

    #logging.info('Completed get billing-service account table data')
    #logging.info('Starting get SMC asset table data')

    #logging.info('Completed get SMC asset table data')
    #logging.info('Starting get tblVSAs table data')

    #tables from 810 database
    #try:
    #    engine = mysql_810
    #    Session = scoped_session(sessionmaker(bind=engine))
    #    s = Session()
        # execute query
    #    src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'sslvpn' and table_name in ('tblVSAs') """)
    #    for tbl in src_tables:
            # query and load save data to dataframe
    #        df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
    #        df = df.replace(r'\\','/', regex=True)
    #        df = df.replace(r'\n',' ', regex=True)
    #        load(df, tbl[0])
    #except Exception as e:
    #    print("Data extract error on 810: " + str(e))

    #logging.info('Completed get tblVSAs table data')
    #logging.info('Starting get tblTransactions table data')

    #tblTransactions
    #try:
    #    engine = mysql_810
    #    Session = scoped_session(sessionmaker(bind=engine))
    #    s = Session()
    #    # execute query
    #    src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'sslvpn' and table_name in ('tblTransactions') """)
    #    for tbl in src_tables:
    #        # query and load save data to dataframe
    #        df = pd.read_sql_query(f'select * FROM {tbl[0]} where unixdate <= 1667293214', engine) #from_unixtime(unixdate) > date_sub(sysdate(), interval 1 day)', engine)
    #        load(df, tbl[0])
    #        df = df.replace(r'\\','/', regex=True)
    #        df = df.replace(r'\n',' ', regex=True)
    #except Exception as e:
    #    print("Data extract error on 810: " + str(e))

    #logging.info('Completed get tblTransactions table data')
    #logging.info('Starting get power_utilizations table data')

    #power_utilizations
    #if time_now >= time(10) and time_now < time(11):
    #    try:
    #        engine = mysql_aux1
    #        Session = scoped_session(sessionmaker(bind=engine))
    #        s = Session()
            # execute query
    #        src_tables = s.execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'power-etl' and table_name in ('power_utilizations') """)
    #        for tbl in src_tables:
                # query and load save data to dataframe
    #            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine) # where created_at > date_sub(sysdate(), interval 1 day)', engine)
    #            df = df.replace(r'\\','/', regex=True)
    #            df = df.replace(r'\n',' ', regex=True)
    #            load(df, tbl[0])
    #    except Exception as e:
    #        print("Data extract error on aux1: " + str(e))
    #else:
    #    pass
    #    print("Skipped procedure as it is not in timeframe")

    #logging.info('Completed get power_utilizations table data')

# load data to s3
def load(df, tbl):
#    logging.info('Starting write table and push to S3')
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'smc/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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

#    logging.info('Completed write table and push to S3')

#logging.warning('Start procedure to get MySQL table data and write to S3 csv files')

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

#logging.info('Completed ETL process to push MySQL data to S3')
