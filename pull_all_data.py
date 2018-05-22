# -*- coding: utf-8 -*-
"""
Created on Thu May  3 20:24:51 2018

@author: micha
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 18:37:43 2018

@author: marti
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 30 18:49:43 2017

@author: marti
"""

import pymysql as pms
import datetime
import time
import pandas as pd
from pandas import DataFrame as df
from time import gmtime, strftime
import sys


host_name = "nwdb.liveimagination.net"
user_name = "nwdblive_salesfo"
pass_word = "#cM6Gb3n{}ut"

database = "nwdblive_furniturerequests"
charset = 'utf8mb4'
cursorclass = pms.cursors.DictCursor

conn = pms.connect(host = host_name, user = user_name                      
                   ,password = pass_word   
                   ,db=database,charset='utf8mb4'
                   ,cursorclass=pms.cursors.DictCursor
                  )
a = conn.cursor()
#a.execute("SELECT * FROM nwdblive_furniturerequests.furniture_requests WHERE ID = 	341 ;")

def fetch_last_run_time():
    outputfile = pd.read_csv(r"c:/temp/NWFB/generallog/log.csv",sep=',',encoding='iso-8859-1')
    print(outputfile.loc[len(outputfile)-1,'LastRun'])
    last_run_time = outputfile.loc[len(outputfile)-1,'LastRun']
    return last_run_time


def connect_to_db(host, user, password,db,chartset, cursorclass):
    charset='utf8mb4'
    cursorclass=pms.cursors.DictCursor
    conn = pms.connect(host = host, user = user, password = password, db=db,charset=charset,cursorclass=cursorclass)
    dbcursor = conn.cursor()
    return dbcursor

def select_client_records(cursor):
    try:
        downloadTime = time.time()
        downloadTime = datetime.datetime.fromtimestamp(downloadTime).strftime('%Y-%m-%d %H:%M:%S')
        print(downloadTime)
        furniture_query = "SELECT * FROM nwdblive_furniturerequests.furniture_requests;"
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
        for index, row in outputdf.iterrows():
            if type(row['WILL_CALL_DELIVERY_DATE']) != pd._libs.tslib.NaTType and row['WILL_CALL_DELIVERY_DATE'] != '0000-00-00' and row['WILL_CALL_DELIVERY_DATE'] != None:
                outputdf.loc[index,'CloseDate'] = str(str(row.WILL_CALL_DELIVERY_DATE.year)+'-'+str(row.WILL_CALL_DELIVERY_DATE.month)+'-'+str(row.WILL_CALL_DELIVERY_DATE.day))
            elif type(row['DELIVERY_DATE']) != pd._libs.tslib.NaTType and row['DELIVERY_DATE'] != '0000-00-00' and row['DELIVERY_DATE'] != 'None' and row['DELIVERY_DATE'] != None:
                outputdf.loc[index,'CloseDate'] = str(str(row.DELIVERY_DATE.year)+'-'+str(row.DELIVERY_DATE.month)+'-'+str(row.DELIVERY_DATE.day))
            elif row['SHOP_DATE'] != None and row['SHOP_DATE'] != '0000-00-00':
                print(row.SHOP_DATE)
                outputdf.loc[index,'CloseDate'] = str(str(row.SHOP_DATE.year)+'-'+str(row.SHOP_DATE.month)+'-'+str(row.SHOP_DATE.day))
            else:
                print(row.SUBMITTED_DATETIME)
                print(index)
                outputdf.loc[index,'CloseDate'] = str(str(row.CREATED.year)+'-'+str(row.CREATED.month)+'-'+str(row.CREATED.day))
#        outputdf['CloseDate'] = [str(str(item.SUBMITTED_DATETIME.year)+'-'+str(item.SUBMITTED_DATETIME.month)+'-'+str(item.SUBMITTED_DATETIME.day)) for index, item in outputdf.iterrows() ]
        outputdf = outputdf.set_index(outputdf['ID'])
    except UnboundLocalError:
        print("No New Records")
        print("closing program")
        theTime = df(columns = {'time'})
        theTime['time'] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
#        theTime.to_csv(r"c:/users/marti/desktop/thetime.csv",sep=',',encoding='iso-8859-1')
        sys.exit()
    return outputdf


def select_furniture_records(cursor):
    try:
        downloadTime = time.time()
        downloadTime = datetime.datetime.fromtimestamp(downloadTime).strftime('%Y-%m-%d %H:%M:%S')
        print(downloadTime)
        furniture_query = "SELECT * FROM nwdblive_furniturerequests.furniture_requests_items"
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
        outputdf = outputdf.set_index(outputdf['REQUESTID'])
        cursor.close()
    except UnboundLocalError:
        print("No New Records")
        print("closing program")
        theTime = df(columns = {'time'})
        theTime['time'] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        theTime.to_csv(r"c:/users/marti/desktop/thetime.csv",sep=',',encoding='iso-8859-1')
        sys.exit()
    return outputdf 


cur = connect_to_db(host_name,user_name,pass_word,database,charset,cursorclass)

records = select_client_records(cur)
#furniture_records = select_furniture_records(cur)

#furniture.to_csv(r"C:/users/marti/desktop/test_db_output.csv",sep=',',encoding = 'iso-8859-1')
#records.to_csv(r"C:/users/micha/desktop/test_db_output_records.csv",sep=',',encoding = 'iso-8859-1')

# Matches records that have been produced through the webapp sign up flow to those in salesforce and produces a list
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
        print(item)
        upload_string = str("{0},{1}").format(item,upload_string)
    print(upload_string)
    print("UPDATE nwfurnit_furniturerequests.furniture_requests SET DOWNLOADED = 1 WHERE ID in ({0});".format(upload_string[:-1]))

    cursor.execute("UPDATE nwfurnit_furniturerequests.furniture_requests SET DOWNLOADED = 1 WHERE ID in ({0}); COMMIT;".format(upload_string[:-1]))
    update_log = cursor.execute("Select ID, downloaded FROM nwfurnit_furniturerequests.furniture_requests WHERE ID in ({0});".format(upload_string[:-1]))
    for a in cursor:
        print(a)
    cursor.close()
    return update_log
