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


test_contacts = pd.read_csv(r"C:\Users\micha\Documents\NWFB\test_examples\contactsFile3.csv")
test2 = test_contacts[['FirstName','LastName','AccountId','Caseworker_Name__c']]

def cleaning_pandas(dataframe):
    ''' removes all nans'''
    for column in test_contacts:
        for index, item in test_contacts.iterrows():
            test_contacts.loc[index,column] = str(test_contacts.loc[index,column]).replace('nan','')
    return dataframe


def insert_job(creds, sf_object, dataset):
    try:
        job = login.create_insert_job("Contact", contentType='JSON', concurrency='Serial')
        cont_dict = contacts.to_json(orient = 'records')
        csv_iter = iter(cont_dict)
        batch = login.post_batch(job, csv_iter)
        print(login.batch_status(batch))
        results = login.batch_status(batch)
        login.wait_for_batch(job, batch)
        login.close_job(job)
        print(sf_object + "s " + "Insert Job Complete")
    except:
        print("error")
        creds.close_job(job)
    return results


login = instance_login(True)
contacts = cleaning_pandas(test2)
results = insert_job(login, "Contact",contacts)


    
    
#insert_job(login,"Contact",contacts)

#job = login.create_insert_job("Contact", contentType='JSON', concurrency='Serial')
#cont_dict = contacts.to_json(orient = 'records')
#csv_iter = iter(cont_dict)
#batch = login.post_batch(job, csv_iter)
#print(login.batch_status(batch))
#login.wait_for_batch(job, batch)
#login.close_job(job)

