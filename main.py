import numpy as np
import pandas as pd
import adal
import requests
from sqlalchemy import create_engine
from datetime import timedelta

# user password for database, please adjust this (username, password, server)
user_db = ''
pass_db = ''
server = ''
database = ''

# create connection using
engine = create_engine('mssql+pyodbc://'+ user_db + ':' + pass_db +'@' + server + '/' + database + '?driver=SQL Server Native Client 11.0', fast_executemany=True)

# api preparation
getDate = pd.to_datetime('today') - timedelta(2) # you can adjust it, it depends on when the data has been provided in azure portal
tenant_id = '' # see the tenant id in azure portal
client_secret = '' # see the client secret in azure portal
client_id = '' # see the client id in azure portal
username = '' # username for login azure
password = '' # password for login azure
authority = "https://login.microsoftonline.com/" + tenant_id
RESOURCE = "https://graph.microsoft.com"
getTeamsUserActivityUserDetail = f"https://graph.microsoft.com/v1.0/reports/getTeamsUserActivityUserDetail(date={getDate.date()})"

try:
    context = adal.AuthenticationContext(authority)

    # Use this for Client Credentials
    token = context.acquire_token_with_client_credentials(
       RESOURCE,
       client_id,
       client_secret
       )

    # Use this for Resource Owner Password Credentials (ROPC)
    token = context.acquire_token_with_username_password(RESOURCE, username, password, client_id)
except Exception as e:
    msg = e

# get data from API getTeamsUserActivityUserDetail
try:
    header = {
        "Authorization":"Bearer " + token['accessToken']}
    w = requests.get(getTeamsUserActivityUserDetail, headers=header)
except Exception as e:
    msg = e

t = w.text.split('\r\n')

df = pd.DataFrame([sub.split(',') for sub in t],)
df.columns = df.iloc[0].str.replace(' ','')
df = df.iloc[1:].reset_index(drop=True)

df.rename(columns={'ScheduledOne-timeMeetingsOrganizedCount':'ScheduledOnetimeMeetingsOrganizedCount', 'ScheduledOne-timeMeetingsAttendedCount':'ScheduledOnetimeMeetingsAttendedCount'}, inplace=True)
df.replace('', np.nan, inplace=True)
df.dropna(how='all', inplace=True)

df[['ReportRefreshDate','LastActivityDate','DeletedDate']] = df[['ReportRefreshDate','LastActivityDate','DeletedDate']].apply(pd.to_datetime, format='%Y-%m-%d')
df = df.astype({'TeamChatMessageCount' : 'int','PrivateChatMessageCount' : 'int','CallCount' : 'int','MeetingCount' : 'int','MeetingsOrganizedCount' : 'int','MeetingsAttendedCount' : 'int','AdHocMeetingsOrganizedCount' : 'int','AdHocMeetingsAttendedCount' : 'int','ScheduledOnetimeMeetingsOrganizedCount' : 'int','ScheduledOnetimeMeetingsAttendedCount' : 'int','ScheduledRecurringMeetingsOrganizedCount' : 'int','ScheduledRecurringMeetingsAttendedCount' : 'int','AudioDurationInSeconds' : 'int','VideoDurationInSeconds' : 'int','ScreenShareDurationInSeconds' : 'int','UrgentMessages' : 'int','PostMessages' : 'int','ReplyMessages' : 'int','ReportPeriod' : 'int'})

#store to database
try:
    df.to_sql('TeamsUserActivityUserDetail',  engine, if_exists='append', index=False, chunksize=10000)
except Exception as e:
   msg = e
