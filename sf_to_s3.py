from simple_salesforce import Salesforce
import requests
import pandas as pd
import io
import boto3
from datetime import date, datetime, time
from io import StringIO
from dotenv import load_dotenv
import os
import logging


logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging.info('Starting Salesforce to S3 ETL process')

load_dotenv('.env')

access_key = os.environ['AWS_KEY']
secret_access_key = os.environ['AWS_SECRET']
s3_bucket = os.environ['BUCKET']
sf_username = os.environ['SALESFORCE_USERNAME']
sf_password = os.environ['SALESFORCE_PASSWORD']
sf_token = os.environ['SALESFORCE_SECURITY_TOKEN']

sf = Salesforce(username=sf_username,password=sf_password, security_token=sf_token)

def load_sf(tbl, query):
    try:
        results=sf.query_all(query)
        df = pd.DataFrame(results['records']).drop(columns='attributes')
        df = df.replace(r'\\','/', regex=True)
        df = df.replace(r'\n',' ', regex=True)
        #df['CloseDate'] = pd.to_datetime(df['CloseDate'])
        #df['CloseDate'] = df['CloseDate'].dt.strftime('%Y-%m-%d')
        #df.to_csv('test.csv')

        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = s3_bucket
        upload_file_key = 'salesforce_backups/' + str(tbl) + f"/{str(tbl)}" + datetime.now().strftime("%Y%m%d")
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
        print("Data extract error on Salesforce: " + str(e))

load_sf("Opportunity", """
    Select
    Id,
    IsDeleted,
    AccountId,
    Name,
    Description,
    StageName,
    Amount,
    Probability,
    CloseDate,
    Type,
    NextStep,
    LeadSource,
    IsClosed,
    IsWon,
    ForecastCategory,
    ForecastCategoryName,
    CampaignId,
    OwnerId,
    CreatedDate,
    CreatedById,
    LastModifiedDate,
    LastModifiedById,
    LastActivityDate,
    LastStageChangeDate,
    ContactId,
    External_Account__c,
    External_Account_Id__c,
    External_Id__c,
    External_Opportunity_Id__c,
    External_Owner__c,
    External_Process_Id__c,
    Primary_Solution_Location__c,
    QT_Link__c,
    QT_MRC_Margin__c,
    QT_MRR__c,
    QT_Months_Until_Profit__c,
    QT_NRC_Margin__c,
    QT_NRR__c,
    QuoteTrackerID__c,
    Contract_Type__c,
    Primary_Datacenter__c,
    Quote_Tracker_ID__c,
    QT_Net_Impact__c,
    Revision_Number__c,
    Area_to_Improve__c,
    Backup__c,
    Facility_Network__c,
    First_Account_Opportunity__c,
    High_Density_Datacenter__c,
    Initial_Quote_Created_At__c,
    Internet__c,
    Managed_SAN__c,
    Monitoring__c,
    NRR__c,
    nServer__c,
    Replacement__c,
    Services__c,
    Term__c,
    Workplace__c,
    Partner_Account__c,
    Partner_Contact__c
    from Opportunity
    where CreatedDate>=LAST_N_YEARS:3
    """)

load_sf("Account", """
    Select 
    Id,
    IsDeleted,
    MasterRecordId,
    Name,
    Type,
    RecordTypeId,
    ParentId,
    Phone,
    Fax,
    AccountNumber,
    Website,
    PhotoUrl,
    Sic,
    Industry,
    AnnualRevenue,
    NumberOfEmployees,
    Ownership,
    TickerSymbol,
    Rating,
    Site,
    OwnerId,
    CreatedDate,
    CreatedById,
    LastModifiedDate,
    LastModifiedById,
    SystemModstamp,
    LastActivityDate,
    LastViewedDate,
    LastReferencedDate,
    IsPartner,
    IsCustomerPortal,
    Lead_Source__c,
    Customer_Status__c,
    Customer_Type__c,
    QuoteTrackerID__c,
    Contract_Name__c,
    QuoteTrackerID_for_url__c,
    Contract_effective_date__c,
    Contract_End_Date__c,
    Auto_Renew_Term__c,
    MRR_Last_Updated__c,
    Customer_Tracker_ID__c
    from Account
    """)


load_sf("User","""
    Select
    Id,
    FirstName,
    LastName,
    Division,
    IsActive,
    ReceivesAdminInfoEmails,
    Alias,
    ForecastEnabled,
    MobilePhone,
    CompanyName,
    Department,
    Email,
    EmailEncodingKey,
    EmployeeNumber,
    Extension,
    Fax,
    ReceivesInfoEmails,
    LanguageLocaleKey,
    LocaleSidKey,
    ManagerId,
    NAME,
    CommunityNickname,
    Phone,
    ProfileId,
    UserRoleId,
    FederationIdentifier,
    TimeZoneSidKey,
    Title,
    Username,
    ContactID,
    PortalRole,
    AccountID,
    CreatedDate
    from User
    """)

load_sf("OpportunityTeamMember","""
     Select
     Id,
     OpportunityAccessLevel,
     OpportunityId,
     TeamMemberRole,
     UserId,
     CreatedDate,
     IsDeleted
     from
     OpportunityTeamMember
     """)

load_sf("RelatedPartner", """
    Select
    Id,
    OwnerId,
    IsDeleted,
    Name,
    CreatedDate,
    CreatedById,
    LastModifiedDate,
    LastModifiedById,
    SystemModstamp,
    LastActivityDate,
    LastViewedDate,
    LastReferencedDate,
    ConnectionReceivedId,
    ConnectionSentId,
    Account__c,
    Related_Partners_ID__c,
    Partner__c,
    Master_Agent_Owner__c,
    Assigned_PAE__c,
    Sub_Agent__c,
    If_Other_Sub_Agent_Please_Specify__c,
    Primary_Solution_Type__c,
    Partner_Lead_Status__c,
    Partner_Lead_Submission_Date_del__c
    from Related_Partners__c
    """)

load_sf("AccountTeamMember", """
    Select
    Id,
    AccountId,
    UserId,
    TeamMemberRole,
    PhotoUrl,
    Title,
    AccountAccessLevel,
    OpportunityAccessLevel,
    CaseAccessLevel,
    ContactAccessLevel,
    CreatedDate,
    CreatedById,
    LastModifiedDate,
    LastModifiedById,
    SystemModstamp,
    IsDeleted
    from AccountTeamMember
    """)


logging.info('Completed ETL process to push Salesforce data to S3')