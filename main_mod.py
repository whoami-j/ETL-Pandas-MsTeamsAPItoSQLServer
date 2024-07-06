import numpy as np
import pandas as pd
import os
#import adal
import requests
from sqlalchemy import create_engine
from datetime import timedelta

# user password for database, please adjust this (username, password, server)
user_db = ''
pass_db = ''
server = ''
database = ''

# create connection using
# engine = create_engine('mssql+pyodbc://'+ user_db + ':' + pass_db +'@' + server + '/' + database + '?driver=SQL Server Native Client 11.0', fast_executemany=True)

def read_files_in_folder(folder_path, file_extension):
    """
    Reads all files with a specific extension in a folder.

    Parameters:
    - folder_path (str): Path to the folder containing the files.
    - file_extension (str): Extension of the files to read (e.g., 'xls', 'xlsx', 'csv').

    Returns:
    - A dictionary where keys are filenames and values are Pandas DataFrames containing the file contents.
    """
    file_data = {}

    # Validate folder path
    if not os.path.isdir(folder_path):
        raise ValueError(f"Folder path '{folder_path}' does not exist or is not a directory.")

    # Iterate through files in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        # Check if the file has the specified extension
        if filename.endswith(f'.{file_extension}'):
            try:
                # Read the file based on its extension
                if file_extension == 'xls' or file_extension == 'xlsx':
                    file_data[filename] = pd.read_excel(file_path)
                elif file_extension == 'csv':
                    file_data[filename] = pd.read_csv(file_path)
                else:
                    raise ValueError(f"Unsupported file extension: {file_extension}")
            except Exception as e:
                print(f"Error reading file '{filename}': {str(e)}")
    
    return file_data

# Example usage:
folder_path = 'C:/Users/jakkitli/Documents/GitHub/ETL-Pandas-MsTeamsAPItoSQLServer/CM_POSTPAID/'
file_extension = 'csv'  # Change this to 'xls' or 'xlsx' as needed
files_data = read_files_in_folder(folder_path, file_extension)
df = pd.concat(files_data,ignore_index=True)

#df.rename(columns={'ScheduledOne-timeMeetingsOrganizedCount':'ScheduledOnetimeMeetingsOrganizedCount', 'ScheduledOne-timeMeetingsAttendedCount':'ScheduledOnetimeMeetingsAttendedCount'}, inplace=True)
df.replace('', np.nan, inplace=True)
df.dropna(how='all', inplace=True)
print(df.head())

#df[['ReportRefreshDate','LastActivityDate','DeletedDate']] = df[['ReportRefreshDate','LastActivityDate','DeletedDate']].apply(pd.to_datetime, format='%Y-%m-%d')
#df = df.astype({'TeamChatMessageCount' : 'int','PrivateChatMessageCount' : 'int','CallCount' : 'int','MeetingCount' : 'int','MeetingsOrganizedCount' : 'int','MeetingsAttendedCount' : 'int','AdHocMeetingsOrganizedCount' : 'int','AdHocMeetingsAttendedCount' : 'int','ScheduledOnetimeMeetingsOrganizedCount' : 'int','ScheduledOnetimeMeetingsAttendedCount' : 'int','ScheduledRecurringMeetingsOrganizedCount' : 'int','ScheduledRecurringMeetingsAttendedCount' : 'int','AudioDurationInSeconds' : 'int','VideoDurationInSeconds' : 'int','ScreenShareDurationInSeconds' : 'int','UrgentMessages' : 'int','PostMessages' : 'int','ReplyMessages' : 'int','ReportPeriod' : 'int'})

#store to database
# try:
#     df.to_sql('TeamsUserActivityUserDetail',  engine, if_exists='append', index=False, chunksize=10000)
# except Exception as e:
#    msg = e
