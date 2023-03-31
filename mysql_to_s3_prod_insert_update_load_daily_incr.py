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
import snowflake.connector

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging.info('Starting MySQL files to S3 ETL process')

load_dotenv('.env')

access_key = os.environ['AWS_KEY']
secret_access_key = os.environ['AWS_SECRET']
s3_bucket = os.environ['BUCKET2']

mysql_acm = create_engine(os.environ['ENG_acm'])

time_now = datetime.now()

sf_conn = snowflake.connector.connect(
    user=os.environ['SF_USER'],
    password=os.environ['SF_PWD'],
    account=os.environ['SF_ACCT'],
    region=os.environ['SF_REG'],
    warehouse = os.environ['SF_WH'],
    database = os.environ['SF_DB'],
    schema = os.environ['SF_SCM'],
    ocsp_fail_open=False
)

def extract():
    src_tbls = sf_conn.cursor().execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'SMC' and table_name in ('ASSET','CHANGE_CONTROL','INCIDENT','TASK','TICKET') """)
    for tbl in src_tbls:
        cur =  sf_conn.cursor().execute(f'select max(created_at), max(updated_at) from {tbl[0]}')
        res = cur.fetchone()
        max_ca = (res[0])
        max_ua = (res[1])

        src_tbls = str(tbl[0])
        src_tbls = src_tbls.lower()

        if src_tbls != 'ticket':
            
            try:
                engine = mysql_acm
                Session = scoped_session(sessionmaker(bind=engine))
                s = Session()
                # execute query
                    # query and load save data to dataframe
                df = pd.read_sql_query(f'select distinct * FROM {src_tbls} WHERE (created_at >= "{max_ca}" OR updated_at >= "{max_ua}")', engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc(df, tbl[0])

                if src_tbls == 'asset':
                    src_tbls_2 = sf_conn.cursor().execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'SMC' and table_name in ('ASSET_IP_INFO') """)
                    for tbl in src_tbls_2:
                        src_tbls_2 = str(tbl[0])
                        src_tbls_2 = src_tbls_2.lower()
                        engine = mysql_acm
                        Session = scoped_session(sessionmaker(bind=engine))
                        s = Session()
                        # execute query
                            # query and load save data to dataframe
                        df = pd.read_sql_query(f'select distinct * FROM {src_tbls_2} WHERE asset_id in (select distinct id from {src_tbls} where created_at >= "{max_ca}" OR updated_at >= "{max_ua}") and (created_at >= "{max_ca}" or updated_at >= "{max_ua}")', engine)
                        df = df.replace(r'\\','/', regex=True)
                        df = df.replace(r'\n',' ', regex=True)
                        load_smc(df, tbl[0])

                if src_tbls == 'change_control':
                    src_tbls_2 = sf_conn.cursor().execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'SMC' and table_name in ('CHANGE_ASSET','CHANGE_CUSTOMER') """)
                    for tbl in src_tbls_2:
                        src_tbls_2 = str(tbl[0])
                        src_tbls_2 = src_tbls_2.lower()
                        engine = mysql_acm
                        Session = scoped_session(sessionmaker(bind=engine))
                        s = Session()
                        # execute query
                            # query and load save data to dataframe
                        df = pd.read_sql_query(f'select distinct * FROM {src_tbls_2} WHERE change_id in (select distinct id from {src_tbls} where created_at >= "{max_ca}" OR updated_at >= "{max_ua}")', engine)
                        print({src_tbls_2})
                        print({src_tbls})
                        print({max_ca})
                        print({max_ua})
                        df = df.replace(r'\\','/', regex=True)
                        df = df.replace(r'\n',' ', regex=True)
                        load_smc(df, tbl[0])

                if src_tbls == 'incident':
                    src_tbls_2 = sf_conn.cursor().execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'SMC' and table_name in ('INCIDENT_ASSET','INCIDENT_CUSTOMER','INCIDENT_FACILITY') """)
                    for tbl in src_tbls_2:
                        src_tbls_2 = str(tbl[0])
                        src_tbls_2 = src_tbls_2.lower()    
                        engine = mysql_acm
                        Session = scoped_session(sessionmaker(bind=engine))
                        s = Session()
                        # execute query
                            # query and load save data to dataframe
                        df = pd.read_sql_query(f'select distinct * FROM {src_tbls_2} WHERE incident_id in (select distinct id from {src_tbls} where created_at >= "{max_ca}" OR updated_at >= "{max_ua}")', engine)
                        df = df.replace(r'\\','/', regex=True)
                        df = df.replace(r'\n',' ', regex=True)
                        load_smc(df, tbl[0])
                
            except Exception as e:
                print("Data extract error on acm: " + str(e))

        else:
            try:
                engine = mysql_acm
                Session = scoped_session(sessionmaker(bind=engine))
                s = Session()
                # execute query
                    # query and load save data to dataframe
                df = pd.read_sql_query(f'select id, status_id, source_id, visibility_id, severity_id, queue_id, customer_id, category_id, problem_id, problem_template_id, template_id, subject, address, hold_end_at, time_held, ttr, ttc, is_escalated, is_phone, created_by, updated_by,assigned_to, reopened_by, held_by, resolved_by, closed_by, created_at, updated_at, internal_updated_at, update_due_at, close_due_at, assigned_at, moved_at, reopened_at, held_at, resolved_at, closed_at, vantive_id, ticket_scheduler_id, ticket_template_id, legacy_id, legacy_system_id, is_survey_accessed, close_out_at, is_awaiting_response, acknowledgement, acknowledged_at, is_master_outage, escalate_for_review_start_at, is_lync, hold_reason_id, sub_problem_id, helpful_yes_count, helpful_no_count, helpful_neutral_count, root_cause_id, root_cause_other, parent_ticket_scheduler_id, max_severity_id, metadata FROM {src_tbls} WHERE created_at >= "{max_ca}" OR (updated_at >= "{max_ua}" and created_at >= "2021-01-01 00:00:00")',engine)
                df = df.replace(r'\\','/', regex=True)
                df = df.replace(r'\n',' ', regex=True)
                load_smc(df, tbl[0])

                if src_tbls == 'ticket':
                    src_tbls_2 = sf_conn.cursor().execute(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'SMC' and table_name in ('TICKET_ASSET') """)
                    for tbl in src_tbls_2:
                        src_tbls_2 = str(tbl[0])
                        src_tbls_2 = src_tbls_2.lower()
                        engine = mysql_acm
                        Session = scoped_session(sessionmaker(bind=engine))
                        s = Session()
                        # execute query
                            # query and load save data to dataframe
                        df = pd.read_sql_query(f'select distinct * FROM {src_tbls_2} WHERE ticket_id in (select distinct id from {src_tbls} where created_at >= "{max_ca}" OR (updated_at >= "{max_ua}" and created_at >= "2021-01-01 00:00:00"))', engine)
                        df = df.replace(r'\\','/', regex=True)
                        df = df.replace(r'\n',' ', regex=True)
                        load_smc(df, tbl[0])        
                    
            except Exception as e:
                print("Data extract error on acm: " + str(e))

# load data to s3
def load_smc(df, tbl):

    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'mysql_backups/' + 'smc/' + 'daily_incr/' + str(tbl).lower() + f"/{str(tbl).lower()}" + datetime.now().strftime("%Y%m%d%H:%M:%S")
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

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))