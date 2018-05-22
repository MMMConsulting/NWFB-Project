# -*- coding: utf-8 -*-
"""
Created on Sun Apr  8 20:00:32 2018

@author: micha
"""

now = datetime.datetime.now()
now_minus_five = now - datetime.timedelta(minutes=5)
now_plus_seven = now_minus_five + datetime.timedelta(hours=7)
now_time = str(now_plus_seven)[0:10] + "T" + str(now_plus_seven)[11:19] + "Z"


test_contact_query = "Select Id,AccountId,Name,Phone,HomePhone,Email,Caseworker_Name__c,CreatedDate,signup_id__c FROM Contact WHERE recordtypeid = '012A0000000GPtUIAW' AND createddate >= 2018-01-01T00:00:00Z"
prod_contact_query = "Select Id,AccountId, Name,Phone,HomePhone,Email,Caseworker_Name__c,CreatedDate,signup_id__c FROM Contact WHERE recordtypeid = '012A0000000GPtUIAW' AND createddate = TODAY"
''' create dynamic version of these queries that pulls records created in last 10 minutes'''
prod_caseworker_query = "Select Id,AccountId, Name,CreatedDate,signup_id__c FROM Contact WHERE recordtypeid = '012A0000000GPtKIAW' AND CreatedDate = TODAY AND signup_id__c != NULL"
prod_opportunity_query = "Select Id, formattedPhoneNumber__c, CreatedDate, signup_id__c FROM opportunity WHERE  CreatedDate = TODAY AND SIGNUP_ID__C != NULL AND CREATEDDATE >= {0}".format(now_time)
test_opportunity_query = "Select Id, formattedPhoneNumber__c, CreatedDate, signup_id__c, closedate, Delivery_Date__c  FROM opportunity"


from salesforce_bulk import SalesforceBulk
import unicodecsv
import time
import datetime
from pandas import DataFrame as df

bulk = SalesforceBulk(username='admin@havensconsulting.net',password='91dU9hsKZdkz',security_token='ATLclvPX1UFxT05UZMIHrAkM')

'''
Queries
'''
def bulk_query(SF_object, query):
    job = bulk.create_query_job(SF_object ,contentType = 'CSV')
    batch = bulk.query(job,query)
    bulk.close_job(job)
    while not bulk.is_batch_done(batch):
        time.sleep(10)
    print(SF_object + " SF Query Complete")
    return bulk, batch
    
def format_dictionary(bulk_data,batch):
    for result in bulk.get_all_results_for_query_batch(batch):
        reader = unicodecsv.DictReader(result, encoding='utf-8')
    headers = []
    data_set = []
    ''' Collect the Dictionary Values
    '''
    for row in reader:
        row_values = []
        for key, value in row.items():
 #           print(key)
 #           print(value)
            row_values.insert(-1,value)
        data_set.insert(0,row_values)
        row_values = []
        
        ''' Collect the Dictionary Keys
        '''
        for header in row.keys():
            if len(headers) == len(data_set[0]):
                pass
            else:
                headers.insert(-1,header)
    output = df(data = data_set, columns = headers)
    print("Data Formatting Complete")
    print(str(len(data_set)) + " Rows of data processed") 
    return output    

c_bulk, c_batch = bulk_query('Opportunity',prod_opportunity_query)
contacts = format_dictionary(c_bulk,c_batch)


