# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 17:27:18 2017

@author: marti

Call map for all scripts involved in the Webform to Salesforce automation

"""

import sys
sys.path.append('c:/temp/NWFB/scripts/etl')

import numpy as np
import pandas as pd
from pandas import DataFrame as df
import logs, databasePull3 as db, sf_queries as sfq, sf_inserts as sfi, lineItemScriptv3 as lis, firstFormatFile_two as fff
import time 
import datetime

sandbox = False

'''
Step 1 - Security function for easy use...
'''

fmt = '%Y-%m-%d %H:%M:%S'
theTime = df(columns = {'time'})
theTime.loc[0,'time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
theTime['time'] = datetime.datetime.now()

'''
Step 2 - call webpage table - hold all new data in memory and add new raw records to a flat file
'''

last_run_time = db.fetch_last_run_time()

cursor = db.connect_to_db(db.host_name, db.user_name, db.pass_word, db.database, db.charset, db.cursorclass)

dataSet = db.select_client_records(cursor)
furniture_items = db.select_furniture_records(cursor)


c_bulk, c_batch = sfq.bulk_query('Contact',sfq.initial_contact_query,sandbox)
initial_contacts = sfq.format_dictionary(c_bulk,c_batch)
''' Check to see if the fields being downloaded are already in salesforce ''' 

dataSet['to_be_dropped'] = 0
for idx, row in dataSet.iterrows():
    for index, item in initial_contacts.iterrows():
        if str(row.ID) == str(item.signup_id__c):
            dataSet.loc[idx,'to_be_dropped'] = 1
            pass
        else:
            pass



if len(dataSet[dataSet['to_be_dropped'] == 0]) == 0:
    print("No new records... Closing Application")
    theTime.to_csv(r"c:/users/martimi/desktop/thetime.csv",sep=',',encoding='8859-1')
    exit()
else:
    print("Running Application")
    pass
'''
Step 3 - Call Format function - Format new data and pass to the next function
      -Format three DFs linked by a common index Primary Key
'''

contact = fff.contactFormat(dataSet)
#contactWLookup = fff.caseAndAgencyTables(contact,fff.caseWorkers,fff.agencies)



'''
Step 4 - Upload New Contacts to Salesforce
        -Drop New Contact file into upload folder
        -Call the Contact Upload script
'''

#contact.to_csv(r"C:\temp\NWFB\uploads\contacts\archive\contacts-"+str(theTime['time'])[0:10]+".csv",sep=',',encoding = 'iso-8859-1')
contact.to_csv(r"C:\temp\NWFB\uploads\contacts\contactsFile.csv",sep=',',encoding = 'iso-8859-1')



''' for testing purposes only'''
#contact['AccountId'] = '0011F000007Y9hzQAC'
#contact['Caseworker_Name__c'] = '0031F000004RzwE'

#contact = contact[contact_upload['to_be_dropped'] == 0]

contact_upload = contact.drop(['CloseDate','to_be_dropped'], axis = 1)
''' remove contacts already in the system''' 


''' remove records whose caseworker has been deleted'''
cw_bulk, cw_batch = sfq.bulk_query('Contact',sfq.prod_full_caseworker_query,sandbox)
caseworkers = sfq.format_dictionary(cw_bulk,cw_batch)

for idx, row in contact_upload.iterrows():
    for index, item in caseworkers.iterrows():
        if str(row.Caseworker_Name__c) == str(item.Id):
            print('match')
            contact_upload.loc[idx,'to_be_dropped2'] = 0
            next
        else:
            pass

contact_upload = contact_upload[contact_upload.to_be_dropped2 == 0]
contact_upload = contact_upload.drop(['to_be_dropped2'], axis = 1)

login = sfi.instance_login(sandbox)
contact_upload = sfi.cleaning_pandas(contact_upload)
con_results = sfi.insert_job(login, "Contact",contact_upload)
print(con_results)

'''
Step 5 - Upload New Opportunities
         -Pull last X contact records from salesforce
         -Append New ContactIDs to the Opportunity Records
         -Drop New Opportunity File into upload folder
         -Call the Opporunity Upload Script
'''
time.sleep(10)

c_bulk, c_batch = sfq.bulk_query('Contact',sfq.prod_contact_query,sandbox)
contactIDs = sfq.format_dictionary(c_bulk,c_batch)

contactIDs['CloseDate'] = ''

for index, item in contactIDs.iterrows():
    for idx, row in contact.iterrows():
        if str(contactIDs.loc[index,'signup_id__c']) == str(contact.loc[idx,'signup_id__c']):
            contactIDs.loc[index,'CloseDate'] = contact.loc[idx,'CloseDate']
            pass
        else:
            print('pass')
            pass

contactIDs.to_csv(r"c:\temp\NWFB\downloads\contacts\contact.csv",sep=',',encoding='iso-8859-1')


opportunityUpload = fff.opportunityFormat(contactIDs)


#opportunityUpload.to_csv(r"C:\temp\NWFB\uploads\transaction\transaction.csv",sep=',',encoding = 'iso-8859-1')

login = sfi.instance_login(sandbox)
opportunityUpload = sfi.cleaning_pandas(opportunityUpload)
opp_results = sfi.insert_job(login, "Opportunity",opportunityUpload)
print(opp_results)

c_bulk, c_batch = sfq.bulk_query('Opportunity',sfq.prod_opportunity_query,sandbox)
opportunityId = sfq.format_dictionary(c_bulk,c_batch)
opportunityId = opportunityId.set_index('signup_id__c')


'''
Step 6 - Upload Product line Items
         -Pull Opportunities associated with the contact IDs and append the opportunity IDs to the Product Line Items
         -Drop Product Line Items into Upload Folder
         -Call ProductLine Items upload script
'''


lineItemPrep = lis.lineItemPrep(furniture_items,lis.productDetails,opportunityId)
lineItemFinal = lis.lineitemprep2(lineItemPrep,lis.productDetails)
lineItemFinal.to_csv(r"C:\temp\NWFB\uploads\lineitems\lineitems.csv",sep=',',encoding='iso-8859-1',index=False)

lineItemUpload = df ( columns = {
                                        'OpportunityId': lineItemFinal['Id']
                                        ,'PricebookEntryId': lineItemFinal['PricebookEntryId']
#                                        ,'Requested__c': lineItemFinal['REQUESTED']
                                        ,'Status__c': lineItemFinal['STATUS']
#                                        ,'Declined__c ': lineItemFinal['Declined__c']
#                                        ,'N_A__c': lineItemFinal['N_A__c']
#                                        ,'Product2Id':''
#                                        ,'Received__c':''
                                    })
    
lineItemUpload['OpportunityId'] = lineItemFinal['Id']
lineItemUpload['PricebookEntryId'] = lineItemFinal['PricebookEntryId']
lineItemUpload['Requested__c']= lineItemFinal['REQUESTED']
lineItemUpload['Status__c'] = lineItemFinal['STATUS']
#lineItemUpload['Product2Id'] = lineItemFinal['Product2Id']
lineItemUpload['Declined__c'] = lineItemFinal['Declined__c']
lineItemUpload['N_A__c'] = lineItemFinal['N_A__c']
lineItemUpload['Received__c'] = lineItemFinal['PROVIDED']

lineItemUpload['Declined__c'] = np.where(lineItemUpload['Declined__c'] == 1, 'true','false')
lineItemUpload['N_A__c'] = np.where(lineItemUpload['N_A__c'] == 1, 'true','false')
lineItemUpload['Status__c'] = np.where(lineItemUpload['Status__c'] == 'Declined',' ',lineItemUpload['Status__c'])
lineItemUpload['Status__c'] = np.where(lineItemUpload['Status__c'] == 'Not Available',' ',lineItemUpload['Status__c'])

login = sfi.instance_login(sandbox)
line_item_upload = sfi.cleaning_pandas(lineItemUpload)
line_item_results = sfi.insert_job(login, "OpportunityLineItem",line_item_upload)
print(line_item_results)

time.sleep(5)
#cp.callUploadLineItems(cp.opDir)

'''
Step 7 - Clean up and Logging
        -Master note file:
                -Push a | delimited row that holds the last date, the number of rows pushed
                -
        -Wrap the uploaded contacts,opps, and non-confidential info into a google sheet for review
'''

#logs.general_logs(theTime['time'],contact)



'''
Step 8
   - Pull the contact list again and compare the active contact list to the userid field 
   - For each Contact record userid that is verified in salesforce, add that userid to a list
   - Use those userids to update the contact table and turn the downloaded field from 0 to 1
'''

commited_ids_list = str(opportunityId.index.tolist())[1:-1]




c_bulk, c_batch = sfq.bulk_query('Contact',sfq.ending_contact_query.format(commited_ids_list) ,sandbox)
commited_contacts = sfq.format_dictionary(c_bulk,c_batch)
print(commited_contacts)
'''Pull Contacts Again '''


cursor2 = db.connect_to_db(db.host_name, db.user_name, db.pass_word, db.database, db.charset, db.cursorclass)

update_values = db.upload_verification(contactIDs,commited_contacts)

update_log = db.upload_db_update(update_values,cursor2)


#Create a spreadsheet that will hold all of the updates values...




#for index, item in targets.iterrows():
 #   for col in contact:
  #      if item.Columns == col:
   #         print(item.Columns + ' ' + col)
    #        targets.loc[index,'match'] = 1
     #   else:
      #      pass
