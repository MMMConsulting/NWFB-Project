# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 19:46:35 2017

@author: marti
"""

import pymysql as pms
from pandas import DataFrame as df
import os
import pandas as pd
import sys

host_name = "nwdb.liveimagination.net"
user_name = "nwdblive_salesfo"
pass_word = "#cM6Gb3n{}ut"
database = "nwdblive_furniturerequests"
charset = 'utf8mb4'
cursorclass = pms.cursors.DictCursor

opDir = 'C:/Temp/NWFB/salesforce.com/Data Loader/bin/'

conn = pms.connect(host = host_name, user = user_name
                      , password = pass_word
                      ,db=database,charset='utf8mb4'
                      ,cursorclass=pms.cursors.DictCursor
                      )

a = conn.cursor()
#a.execute("SELECT * FROM nwfurnit_furniturerequests.requested_access WHERE downloaded = 0 AND approved = 1;")
#a.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='nwfurnit_furniturerequests' AND TABLE_NAME='furniture_requests'")
#a.execute("UPDATE nwfurnit_furniturerequests.requested_access SET DOWNLOADED = 0 WHERE downloaded = 1 AND ID IN (3,4); COMMIT;")
         
#outputlist = []
#for row in a:
#    print(row)
#    input = str(row).split("ME': ")
#    outputlist.insert(0,input[1])
#for row in a:
#    print(row)
#    outputlist.insert(0,row)
#test_df = df(columns = ['column_name'], data = outputlist)
#test_df.to_csv("c:/users/marti/desktop/caseworkers_11_14.csv")


def connect_to_db(host, user, password,db,chartset, cursorclass):
    '''
    Connects to database
    '''
    charset='utf8mb4'
    cursorclass=pms.cursors.DictCursor
    conn = pms.connect(host = host, user = user, password = password, db=db,charset=charset,cursorclass=cursorclass)
    dbcursor = conn.cursor()
    return dbcursor

def select_caseworker_records(cursor):
    '''
    Pulls Caseworker records and formats them into pandas dataframe
    '''
    try:
#        downloadTime = time.time()
#        downloadTime = datetime.datetime.fromtimestamp(downloadTime).strftime('%Y-%m-%d %H:%M:%S')
#        print(downloadTime)
        furniture_query = "SELECT * FROM nwdblive_furniturerequests.requested_access WHERE downloaded = 0 AND APPROVED = 1"
        print(furniture_query)
        cursor.execute(furniture_query)
        row_values = []
        column_values = []
        output_data = []
        for row in cursor:
            for item in row.items(): #Extracting Values
                row_values.insert(0,item[1])
            output_data.insert(0,row_values)
            row_values = []
        for item2 in row.keys(): #Extracting Columns
            column_values.insert(0,item2)
        outputdf = df(data = output_data, columns = column_values)
        outputdf = outputdf.set_index(outputdf['ID'])
        cursor.close()
        outputdf['recordtypeid'] = '012A0000000GPtKIAW'
        outputdf['Caseworker_Name'] = outputdf['CASEWORKER_FIRST'] + " " + outputdf['CASEWORKER_LAST']
        return outputdf
    except UnboundLocalError: 
        cursor.close()
        print("No New Records")
        sys.exit()

def upload_verification(contact_df,client_query_output):
    update_values = []
    for index, row in contact_df.iterrows():
        for index2, row2 in client_query_output.iterrows():
            if row['ID'] == row2['SIGNUP_ID__C']:
                update_values.insert(0,row['ID'])
    return update_values

#Accepts the list from upload_verification and set the downloaded field from 0 to 1
def upload_db_update(update_values,cursor):
    upload_string = ''
#    update_query = "UPDATE nwfurnit_furniturerequests.furniture_requests SET downloaded = 1 WHERE ID in ({0})"
    for item in update_values:
        print("this is a thing!")
        print(item)
        upload_string = str("{0},{1}").format(item,upload_string)
    print(upload_string)
    print("this is the second thing!!!")
    print("UPDATE nwdblive_furniturerequests.requested_access SET DOWNLOADED = 1 WHERE ID in ({0}); COMMIT;".format(upload_string[:-1]))

    cursor.execute("UPDATE nwdblive_furniturerequests.requested_access SET DOWNLOADED = 1 WHERE ID in ({0}); COMMIT;".format(upload_string[:-1]))
    update_log = cursor.execute("Select ID, downloaded FROM nwfurnit_furniturerequests.requested_access WHERE ID in ({0}); COMMIT;".format(upload_string[:-1]))
    for a in cursor:
        print(a)
    cursor.close()
    return update_log


def callUploadCaseWorker(directory):
    print("Uploading Caseworkers...")
    os.chdir(directory)
    output = os.system('process ../samples/conf/ caseworkerInsertProcess | clip')
    print(output)
    print("...Closing Upload")

def callDownloadCaseWorker(directory):
    print("Downloading Caseworkers...")
    os.chdir(directory)
    output = os.system('process ../samples/conf/ caseworkerPullProcess | clip')
    print(output)
    print("...Closing Download")
    
host_name = "nwdb.liveimagination.net"
user_name = "nwdblive_salesfo"
pass_word = "#cM6Gb3n{}ut"
database = "nwdblive_furniturerequests"
charset = 'utf8mb4'
cursorclass = pms.cursors.DictCursor

cursor = connect_to_db(host = host_name, user = user_name, password = pass_word ,db = database ,chartset = charset, cursorclass = cursorclass)

output = select_caseworker_records(cursor)

output.to_csv(r"C:/temp/NWFB/uploads/caseworker/caseworker.csv",sep=',',encoding='iso-8859-1')

callUploadCaseWorker(opDir)
''' Pull caseworkers again... '''
callDownloadCaseWorker(opDir)

salesforce_caseworkers = pd.read_csv(r"C:/temp/NWFB/downloads/caseworker/caseworker.csv",sep=',',encoding='iso-8859-1')
upload_check = upload_verification(output,salesforce_caseworkers)
cursor2 = connect_to_db(host = host_name, user = user_name, password = pass_word ,db = database ,chartset = charset, cursorclass = cursorclass)

updatelog = upload_db_update(upload_check,cursor2)


# Validate that the records were uploaded and marked them as downloaded in the database