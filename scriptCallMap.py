# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 17:27:18 2017

@author: marti

Call map for all scripts involved in the Webform to Salesforce automation

"""
import sys
sys.path.append('c:/temp/NWFB/scripts/etl')

import pandas as pd
from pandas import DataFrame as df
import callWebPage as cwp, logs, databasePull3 as db,contactUpload as cp, lineItemScriptv3 as lis, firstFormatFile_two as fff
import time 
janeDoe = r"C:\temp\NWFB\sampleData\janeDoe.csv"

'''
Step 1 - Security function for easy use...
'''

'''
Step 2 - call webpage table - hold all new data in memory and add new raw records to a flat file
'''

last_run_time = db.fetch_last_run_time()

cursor = db.connect_to_db(db.host_name, db.user_name, db.pass_word, db.database, db.charset, db.cursorclass)

dataSet = db.select_client_records(cursor)
furniture_items = db.select_furniture_records(cursor)

theTime = df(columns = {'time'})
theTime.loc[0,'time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
theTime['time'] = theTime['time'].values[0].replace(' ','T')

if len(dataSet) == 0:
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

contact.to_csv(r"C:\temp\NWFB\uploads\contacts\archive\contacts-"+str(theTime['time'][0][0:10])+".csv",sep=',',encoding = 'iso-8859-1')
contact.to_csv(r"C:\temp\NWFB\uploads\contacts\contactsFile.csv",sep=',',encoding = 'iso-8859-1')
cp.callUploadContact(cp.opDir)


'''
Step 5 - Upload New Opportunities
         -Pull last X contact records from salesforce
         -Append New ContactIDs to the Opportunity Records
         -Drop New Opportunity File into upload folder
         -Call the Opporunity Upload Script
'''
time.sleep(10)
cp.callDownloadContact(cp.opDir)
contactIDs = pd.read_csv(r"c:\temp\NWFB\downloads\contacts\contact.csv",sep=',',encoding='iso-8859-1')
contactIDs = contactIDs

opportunityUpload = fff.opportunityFormat(contactIDs,contact,theTime['time'][0])
opportunityUpload.to_csv(r"C:\temp\NWFB\uploads\transaction\transaction.csv",sep=',',encoding = 'iso-8859-1')
cp.callUploadTransaction(cp.opDir)
cp.callDownloadOpportunity(cp.opDir)
opportunityId = pd.read_csv(r"C:\temp\NWFB\downloads\Opportunity\opportunity.csv",sep=',',encoding='iso-8859-1')
opportunityId = opportunityId.set_index('SIGNUP_ID__C').copy()

'''
Step 6 - Upload Product line Items
         -Pull Opportunities associated with the contact IDs and append the opportunity IDs to the Product Line Items
         -Drop Product Line Items into Upload Folder
         -Call ProductLine Items upload script
'''

lineItemPrep = lis.lineItemPrep(furniture_items,lis.productDetails,opportunityId)
lineItemFinal = lis.lineitemprep2(lineItemPrep,lis.productDetails)
lineItemFinal.to_csv(r"C:\temp\NWFB\uploads\lineitems\lineitems.csv",sep=',',encoding='iso-8859-1',index=False)



time.sleep(5)
cp.callUploadLineItems(cp.opDir)

'''
Step 7 - Clean up and Logging
        -Master note file:
                -Push a | delimited row that holds the last date, the number of rows pushed
                -
        -Wrap the uploaded contacts,opps, and non-confidential info into a google sheet for review
'''

logs.general_logs(theTime['time'][0],contact)



'''
Step 8
   - Pull the contact list again and compare the active contact list to the userid field 
   - For each Contact record userid that is verified in salesforce, add that userid to a list
   - Use those userids to update the contact table and turn the downloaded field from 0 to 1
'''
cp.callDownloadContact(cp.opDir)
contactDownload = pd.read_csv(r"c:\temp\NWFB\downloads\contacts\contact.csv",sep=',',encoding='iso-8859-1')

'''Pull Contacts Again '''
cursor2 = db.connect_to_db(db.host_name, db.user_name, db.pass_word, db.database, db.charset, db.cursorclass)

update_values = db.upload_verification(contact,contactDownload)

update_log = db.upload_db_update(update_values,cursor2)


#Create a spreadsheet that will hold all of the updates values...





