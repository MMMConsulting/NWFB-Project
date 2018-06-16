# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 22:15:09 2018

@author: micha
"""

from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
import unicodecsv
import time
import pandas as pd
from pandas import DataFrame as df


#prod_bulk = SalesforceBulk(username='admin@havensconsulting.net',password='91dU9hsKZdkz',security_token='ATLclvPX1UFxT05UZMIHrAkM')
#test_bulk = SalesforceBulk(username='michael@havensconsulting.net.mikesde',password='P@tersin1',sandbox=True, security_token='K7FjFIUW3KuMXKFthijArNDFP')#
#account = pd.read_csv(r"C:\Users\micha\Desktop\accounts_for_sandbox.csv")


def instance_login(sandbox):
    if sandbox == True:
        creds = SalesforceBulk(username='michael@havensconsulting.net.mikesde',password='P@tersin1',sandbox=True, security_token='K7FjFIUW3KuMXKFthijArNDFP')
        print("Sandbox Login Successful!")
    elif sandbox == False:
        creds = SalesforceBulk(username='admin@havensconsulting.net',password='91dU9hsKZdkz',security_token='ATLclvPX1UFxT05UZMIHrAkM')
        print("Product Login Successful!")
    else: 
        print("Please Input True or False")
    return creds


#test_contacts = pd.read_csv(r"C:\Users\micha\Documents\NWFB\test_examples\contactsFile3.csv")
#test2 = test_contacts[['FirstName','LastName','AccountId','Caseworker_Name__c']]

def cleaning_pandas(dataframe):
    ''' removes all nans'''
    for column in dataframe:
        for index, item in dataframe.iterrows():
            dataframe.loc[index,column] = str(dataframe.loc[index,column]).replace('nan','')
    return dataframe


def insert_job(creds, sf_object, dataset):
    #try:
    print('ya made it here kid')
    job = creds.create_insert_job(sf_object, contentType='JSON', concurrency='Serial')
    cont_dict = dataset.to_json(orient = 'records')
    print('oi keep going')
    csv_iter = iter(cont_dict)
    batch = creds.post_batch(job, csv_iter)
    print('i member atst')
    print(creds.batch_status(batch))
    print('error there')
#        for i in range(wait):
 #           time.sleep(1)
  #          print(creds.batch_status(batch))
#        results = creds.batch_status(batch)
#        time.sleep(10)
    print('error here')
    creds.wait_for_batch(job, batch)
    results = creds.batch_status(batch)
    print('maybe error here')
    creds.close_job(job)
    print(sf_object + "s " + "Insert Job Complete")
    return results
#        return results
#    except:
#        print("error")
 #       creds.close_job(job)
  #  if results == 0:
   #     pass
   # else:
    #    return results
    

 

#login = instance_login(True)
#contacts = cleaning_pandas(account)
#results = insert_job(login, "Account",contacts)

#columns2 = []
#values = []
#for item in results.keys():
#    columns2.insert(0,item)
#for item in results.values():
#    print(item)
#    values.insert(0,item)
    
#results2 = df(columns = columns2, data = values)
    
#insert_job(login,"Contact",contacts)

#job = login.create_insert_job("Contact", contentType='JSON', concurrency='Serial')
#cont_dict = contacts.to_json(orient = 'records')
#csv_iter = iter(cont_dict)
#batch = login.post_batch(job, csv_iter)
#print(login.batch_status(batch))
#login.wait_for_batch(job, batch)
#login.close_job(job)

