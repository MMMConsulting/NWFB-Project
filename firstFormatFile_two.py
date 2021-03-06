# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 21:35:42 2017


@author: marti

This script will begin putting the update file 
"""

import pandas as pd
import numpy as np
from pandas import DataFrame as df
import datetime

recordtypeid = '012A0000000GPtU'
'''
Conditional Logic:
paid__c = 'pending' | 'invoicing' IF Client, Agency, & Other Payee values are ALL BLANK. 
Invoicing if there is a dollar value in ANY
Paid_By__c = Client | Agency | Client and Agency
Number_of_Household_Adults__c = Total Served - Number of CHildren
Other_Contact_1_Relation__c = Provide this field if there is a value in the Other Payee field
'''
#import caseworker list --Ideally point the program at the mySQL tables
caseWorkers = pd.read_csv(r"c:/temp/NWFB/sampleData/contactData.csv",sep=',',encoding = 'iso-8859-1', low_memory=False)
agencies = pd.read_csv(r"c:/temp/NWFB/sampleData/agencyExtract.csv",sep=',',encoding = 'iso-8859-1',low_memory=False)

def phoneIndex(number):
    outputNumber = number.replace('(','')
    outputNumber = outputNumber.replace(')','')
    outputNumber = outputNumber.replace('-','')
    outputNumber = outputNumber.replace('.','')
    outputNumber = outputNumber.replace(' ','')
    outputNumber = '{0}-{1}-{2}'.format(str(outputNumber[0:3]),str(outputNumber[3:6]),str(outputNumber[6::]))
    return outputNumber



def contactFormat(dataset):
    dataset['formattedNumber'] = ''
    for index, item in dataset.iterrows():
        dataset.loc[index,'formattedNumber'] = phoneIndex(item['HomePhone'])
        dataset.loc[index,'CASEWORKER_PHONE'] = phoneIndex(item['CASEWORKER_PHONE'])
        dataset.loc[index,'HomePhone'] = phoneIndex(item['HomePhone'])
        dataset.loc[index,'OtherPhone'] = phoneIndex(item['OtherPhone'])
        dataset.loc[index,'Other_Contact_1_Phone__c'] = phoneIndex(item['Other_Contact_1_Phone__c'])        
    print(dataset['formattedNumber'])
    dataset = dataset.replace(np.NaN,'')
    dataset.OtherPhone = dataset.OtherPhone.replace("--","")
    dataset.Other_Contact_1_Phone__c = dataset.Other_Contact_1_Phone__c.replace("--","")

#    print("poo")
    dataset['recordtypeid'] = '012A0000000GPtU'
#    dataset['firstName'] = ''
#    dataset['lastName'] = ''
#    for index, row in dataset.iterrows():
#        check = str(row['Client Name']).split(' ')
#        dataset.loc[index,['firstName','lastName']] = [check[0],check[1]]
    contactRecord = df(columns = {
#                                'PAID_BY_CLIENT__C_METHOD':''
#                                ,'PAID_BY_AGENCY__C_METHOD':''
                                'Has_Bedbugs__c':''
#                                ,'AGREEMENT_ACCEPTED_DATETIME':''
#                                ,'CLIENT_SIGNATURE':''
#                                ,'CASE_WORKER_SIGNATURE':''
                                ,'Notes__c':''
                                ,'Annual_Family_Income__c':''
                                ,'Homeless_In_The_Last_Month__c':''
                                ,'Veteran__c':''
                                ,'Primary_Reason__c':''
                                ,'Birthdate':''
                                ,'Race__c':''
                                ,'Gender__c':''
                                ,'Children__c':''
                                ,'Total_Served__c':''
                                ,'Other_Contact_1_Phone__c':''
                                ,'Other_Contact_1_Email__c':''
                                ,'Other_Payee_Name__c':''
                                ,'Third_Party_Amount__c':''
                                ,'Paid_by_Agency__c':''
                                ,'Paid_by_Client__c':''
                                ,'Caseworker_Name__c':''
#                                ,'CaseworkerId':''
                                ,'Preferred_Contact__c':''
#                                ,'AgencyName':''
                                ,'AccountId':''
                                ,'Other_Payee_Zip__c':''
                                ,'Other_Payee_State__c':''
                                ,'Other_Payee_City__c':''
                                ,'Other_Payee_Address__c':''
                                ,'Delivery_Zip__c':''
                                ,'Delivery_State__c':''
                                ,'Delivery_City__c':''
#                                ,'DELIVERY_ADDRESS3':''
#                                ,'DELIVERY_ADDRESS2':''
                                ,'apartment_name__c':''
                                ,'Delivery_Street__c':''
                                ,'Delivery__c':''
                                ,'MailingPostalCode':''
                                ,'County__c':''
                                ,'MailingCity':''
#                                ,'MailingStreet3':''
 #                               ,'MailingStreet2':''
                                ,'MailingStreet':''
                                ,'npe01__HomeEmail__c':''
                                ,'OtherPhone':''
                                ,'HomePhone':''
                                ,'LastName':''
                                ,'FirstName':''
                                ,'signup_id__c':''
                                ,'RecordTypeId': ''
                                ,'Will_Call_Tag_Delivery__c':''
                                ,'Will_Call_Delivery_Date__c':''
                                ,'Will_Call_Delivery_Notes__c':''
                                ,'Will_Call_Delivery_Loaded_By__c':''
                                ,'Will_Call_Delivery__c':''
                                ,'Will_Call_Delivery_Shopper__c':''
                                ,'HMIS__c':''
                                ,'to_be_dropped':''
                                }, index = dataset.ID
                    )
    dataset = dataset.dropna(axis = 0, how = 'all')
#    contactRecord.set_index(dataset['formattedNumber'])
#    print(dataset)
    for index2, item in dataset.iterrows():
        contactRecord.loc[index2,'Paid_By__c'] = ' '
        ''' CONTACT INFO ''' 
        contactRecord.loc[index2,'FirstName'] = dataset.loc[index2,'FirstName']
        contactRecord.loc[index2,'LastName'] = dataset.loc[index2,'LastName']
        contactRecord.loc[index2,'County__c'] = dataset.loc[index2,'County__c']
        contactRecord.loc[index2,'HomePhone'] = dataset.loc[index2,'HomePhone']
        contactRecord.loc[index2,'OtherPhone'] = dataset.loc[index2,'OtherPhone']
        contactRecord.loc[index2,'npe01__HomeEmail__c'] = dataset.loc[index2,'Email']
        contactRecord.loc[index2,'Caseworker_Name__c'] = dataset.loc[index2,'CaseworkerId']
        
        
        contactRecord.loc[index2,'CloseDate'] =  dataset.loc[index2,'CloseDate']
        contactRecord.loc[index2,'AccountId'] = dataset.loc[index2,'AgencyId']     
        
        contactRecord.loc[index2,'Total_Served__c'] = dataset.loc[index2,'Total_Served__c']
        contactRecord.loc[index2,'Children__c'] = dataset.loc[index2,'Children__c']
        contactRecord.loc[index2,'Number_of_Household_Adults__c'] = dataset.loc[index2,'Total_Served__c'] - dataset.loc[index2,'Children__c']
        contactRecord.loc[index2,'Gender__c'] = dataset.loc[index2,'Gender__c']
        contactRecord.loc[index2,'Race__c'] = dataset.loc[index2,'Race__c']
        contactRecord.loc[index2,'Birthdate'] = dataset.loc[index2,'Birthdate']
        contactRecord.loc[index2,'Primary_Reason__c'] = dataset.loc[index2,'Primary_Reason__c']
        contactRecord.loc[index2,'Veteran__c'] = dataset.loc[index2,'Veteran']
        contactRecord.loc[index2,'Homeless_In_The_Last_Month__c'] = dataset.loc[index2,'Homeless_In_The_Last_Month__c']
        contactRecord.loc[index2,'Annual_Family_Income__c'] = dataset.loc[index2,'Annual_Family_Income__c']
        contactRecord.loc[index2,'Notes__c'] = dataset.loc[index2,'Notes__c']


        ''' ADDRESS INFO '''
        contactRecord.loc[index2,'MailingStreet'] = dataset.loc[index2,'MailingStreet'] + " #" + dataset.loc[index2,'MailingStreet3']
        contactRecord.loc[index2,'apartment_name__c'] = dataset.loc[index2,'MailingStreet2']
        contactRecord.loc[index2,'MailingCity'] = dataset.loc[index2,'MailingCity']
        contactRecord.loc[index2,'MailingPostalCode'] = dataset.loc[index2,'MailingPostalCode']
        contactRecord.loc[index2,'MailingState'] = 'WA'
        contactRecord.loc[index2,'MailingCountry'] = 'USA'
        
        contactRecord.loc[index2,'Other_Payee_Zip__c'] = dataset.loc[index2,'OTHER_PAYEE_ZIP']
        contactRecord.loc[index2,'Other_Payee_State__c'] = dataset.loc[index2,'OTHER_PAYEE_STATE']
        contactRecord.loc[index2,'Other_Payee_City__c'] = dataset.loc[index2,'OTHER_PAYEE_CITY']
#        contactRecord.loc[index2,'OTHER_PAYEE_ADDRESS2'] = dataset.loc[index2,'OTHER_PAYEE_ADDRESS2']
        contactRecord.loc[index2,'Other_Payee_Address__c'] = dataset.loc[index2,'OTHER_PAYEE_ADDRESS2'] + ' ' + dataset.loc[index2,'OTHER_PAYEE_ADDRESS2']
        contactRecord.loc[index2,'Delivery_Zip__c'] = dataset.loc[index2,'DELIVERY_ZIP']
        contactRecord.loc[index2,'Delivery_State__c'] = dataset.loc[index2,'DELIVERY_STATE']
        contactRecord.loc[index2,'Delivery_City__c'] = dataset.loc[index2,'DELIVERY_CITY']
        contactRecord.loc[index2,'Delivery_Street__c'] = dataset.loc[index2,'DELIVERY_ADDRESS'] + " " + dataset.loc[index2,'DELIVERY_ADDRESS2'] + " " + dataset.loc[index2,'DELIVERY_ADDRESS3']
 
        
        ''' WILL CALL INFO '''
        contactRecord.loc[index2,'Will_Call_Tag_Delivery__c'] = dataset.loc[index2,'WILL_CALL_DELIVERY_TAG_COLOR']
        contactRecord.loc[index2,'Will_Call_Delivery_Date__c'] = dataset.loc[index2,'WILL_CALL_DELIVERY_DATE']
        contactRecord.loc[index2,'Will_Call_Delivery_Notes__c'] = dataset.loc[index2,'WILL_CALL_DELIVERY_NOTES']
        contactRecord.loc[index2,'Will_Call_Delivery_Loaded_By__c'] = dataset.loc[index2,'WILL_CALL_DELIVERY_LOADED_BY']
        contactRecord.loc[index2,'Will_Call_Delivery__c'] = dataset.loc[index2,'WILL_CALL_DELIVERY_DELIVERED_BY']
        contactRecord.loc[index2,'Will_Call_Delivery_Shopper__c'] = dataset.loc[index2,'WILL_CALL_DELIVERY_SHOPPER']
        contactRecord.loc[index2,'HMIS__c'] = dataset.loc[index2,'HMIS']
        contactRecord.loc[index2,'Language__c'] = dataset.loc[index2,'PREFERRED_LANGUAGE'] 
 
        ''' BILLING INFO ''' 
        contactRecord.loc[index2,'Paid_by_Client__c'] = dataset.loc[index2,'Paid_by_Client__c']
        contactRecord.loc[index2,'Paid_by_Agency__c'] = float(dataset.loc[index2,'Paid_by_Agency__c'])
        contactRecord.loc[index2,'Third_Party_Amount__c'] = float(dataset.loc[index2,'Paid_by_Agency__c2'])
        contactRecord.loc[index2,'Received_Services__c'] = 'Served'

        contactRecord.loc[index2,'Other_Contact_1__c'] = dataset.loc[index2,'Other_Contact_1__c']
        contactRecord.loc[index2,'Other_Contact_1_Email__c'] = dataset.loc[index2,'Other_Contact_1_Email__c']
        contactRecord.loc[index2,'Other_Contact_1_Phone__c'] = dataset.loc[index2,'Other_Contact_1_Phone__c']

        contactRecord.loc[index2,'RecordTypeId'] = '012A0000000GPtU'
 #       contactRecord.loc[index2,'formattedNumber'] = dataset.loc[index2,'formattedNumber']
        contactRecord.loc[index2,'Delivery__c'] = dataset.loc[index2,'DELIVERY']
#        contactRecord.loc[index2,'CASEWORKER_EMAIL'] = dataset.loc[index2,'CASEWORKER_EMAIL']
#        contactRecord.loc[index2,'CASEWORKER_PHONE'] = dataset.loc[index2,'CASEWORKER_PHONE']
 #       contactRecord.loc[index2,'Caseworker_Name__c'] = dataset.loc[index2,'CaseworkerName']
        contactRecord.loc[index2,'Other_Payee_Name__c'] = dataset.loc[index2,'OTHER_PAYEE_FIRST'] + " " + dataset.loc[index2,'OTHER_PAYEE_LAST']
        contactRecord.loc[index2,'signup_id__c'] = dataset.loc[index2,'ID']
#        contactRecord.loc[index2,'PAID_BY_CLIENT__C_METHOD'] = dataset.loc[index2,'PAID_BY_CLIENT__C_METHOD']
#        contactRecord.loc[index2,'PAID_BY_AGENCY__C_METHOD'] = dataset.loc[index2,'PAID_BY_AGENCY__C_METHOD']
        contactRecord.loc[index2,'Has_Bedbugs__c'] = dataset.loc[index2,'BED_BUGS_PROBLEM']
#        contactRecord.loc[index2,'AGREEMENT_ACCEPTED_DATETIME'] = dataset.loc[index2,'AGREEMENT_ACCEPTED_DATETIME']
#        contactRecord.loc[index2,'CLIENT_SIGNATURE'] = dataset.loc[index2,'CLIENT_SIGNATURE']
#        contactRecord.loc[index2,'CASE_WORKER_SIGNATURE'] = dataset.loc[index2,'CASE_WORKER_SIGNATURE']
        contactRecord.loc[index2,'Paid__c'] = 'Fee Collected'
#        if contactRecord.loc[index2,'Paid_by_Client__c'] > 0:
#            contactRecord.loc[index2,'Paid__c'] = 'Pending'
#        elif (contactRecord.loc[index2,'Paid_by_Client__c'] == 0) == True & (contactRecord.loc[index2,'Paid_by_Agency__c'] == 0) == True & (contactRecord.loc[index2,'Other_Contact_1__c'] == 0) == True:
#            contactRecord.loc[index2,'Paid__c'] = 'Pending'
#        else:
#            contactRecord.loc[index2,'Paid__c'] = 'Invoicing'
        if (contactRecord.loc[index2,'Paid_by_Client__c'] != ' ') == True & ((contactRecord.loc[index2,'Paid_by_Agency__c'] != ' ') == True | (contactRecord.loc[index2,'Other_Contact_1__c'] != 0)) == True:
            contactRecord.loc[index2,'Paid_By__c'] = 'Client and Agency'
        elif contactRecord.loc[index2,'Paid_by_Client__c'] != ' ':
            contactRecord.loc[index2,'Paid_By__c'] = 'Client'
            contactRecord.loc[index2,'Paid_by_Agency__c'] = ''
        elif (contactRecord.loc[index2,'Paid_by_Agency__c'] != ' ') == True | (contactRecord.loc[index2,'Other_Contact_1__c'] != 0) == True:
            contactRecord.loc[index2,'Paid_By__c'] = 'Agency'
            contactRecord.loc[index2,'Paid_by_Client__c'] = ''
        else:
#            contactRecord.loc[index2,'Paid_by__c'] = 'We have a problem' #remove in the production version
            print('we have a problem')
    for ix, index in contactRecord.iterrows():
        if index['Veteran__c'] == 1:
            contactRecord.loc[ix,'Veteran__c'] = 'true'
        else:
            contactRecord.loc[ix,'Veteran__c'] = 'false'
        if index['Has_Bedbugs__c'] == 1:
            contactRecord.loc[ix,'Has_Bedbugs__c'] = 'true'
        else:
            contactRecord.loc[ix,'Has_Bedbugs__c'] = 'false'
        if index['Homeless_In_The_Last_Month__c'] == 1:
            contactRecord.loc[ix,'Homeless_In_The_Last_Month__c'] = 'true'
        else:
            contactRecord.loc[ix,'Homeless_In_The_Last_Month__c'] = 'false'

    return contactRecord


def caseAndAgencyTables(contactRecord,caseworker,agency):
#    for index, row in contactRecord.iterrows():
#        contactRecord.loc[index,'Caseworker_Name__c'] = [item['ID'] for ix, item in caseworker.iterrows() if item['Caseworker Name'] == row['Caseworker_Name']]
#        contactRecord.loc[index,'Caseworker_Name__c'] = contactRecord.loc[index,'Caseworker_Name__c']
#        contactRecord.loc[index,'Caseworker_Name__c'] = contactRecord.loc[index,'Caseworker_Name__c'][0]
#        contactRecord.loc[index,'Caseworker_Phone__c'] = [item['CaseWorker Phone']  for ix, item in caseworker.iterrows() if item['Caseworker Name'] == row['Caseworker_Name']]
#        contactRecord.loc[index,'Caseworker_Phone__c'] = contactRecord.loc[index,'Caseworker_Phone__c'][0]
#    for ix, row in contactRecord.iterrows():
#        contactRecord.loc[ix,'Caseworker_Phone__c'] = phoneIndex(row['Caseworker_Phone__c'])
    print("here is the contact record")    
    print(contactRecord)
#    for index2, row2 in contactRecord.iterrows():
#        if len(row2.CaseworkerName) > 2:
#            print("this is the name " + row2.FirstName + " " + row2.CaseworkerName)
#            print("this is the id" + str([item['ID'] for ix, item in agency.iterrows() if item['NAME'] == row2['AgencyName']]))
#            contactRecord.loc[index2,'AccountId'] = [item['ID'] for ix, item in agency.iterrows() if item['NAME'] == row2['AgencyName']]
#            print(contactRecord.loc[index2,'AccountId'])
#            contactRecord.loc[index2,'AccountId'] = contactRecord.loc[index2,'AccountId'][0].replace("']",'')
#        else:
#            pass
    contactRecord['ID'] = contactRecord.index.tolist()
    return contactRecord

def opportunityFormat(contactIds):
    contactIds['Type'] = 'Individual'
    contactIds['StageName'] = 'Completed'
    contactIds['RecordTypeId'] = '012A0000000GQ32IAG'
    contactIds['npe01__Contact_Id_for_Role__c'] = contactIds['Id']
    contactIds['Name'] = contactIds['LastName'] + ' ' + contactIds['Delivery__c']
    
#    for index, row in contactData.iterrows():
 #       contactData.loc[index,'accountId'] = contactIds['ACCOUNTID'][contactIds.SIGNUP_ID__C == index].values[0]
  #      contactData.loc[index,'contactId'] = contactIds['CLIENTSFID'][contactIds.SIGNUP_ID__C == index].values[0]
   #     contactData.loc[index,'Name'] = str(row['LastName'] + " " + row['DELIVERY'])
                      
#    for index2, row in opportunityDf.iterrows():
#        print(index2)
#        print(contactIds['CLIENTSFID'][contactIds.index == index2].values[0])
#        opportunityDf.loc[index2,'contactId'] = contactIds['CLIENTSFID'][contactIds.index == index2].values[0]
#        opportunityDf.loc[index2,'accountId'] = contactIds['ACCOUNTID'][contactIds.index == index2].values[0]
#        check = str(str(contactData['LastName'][contactData['ID'] == index2].values[0]) + " " + contactData['DELIVERY'][contactData['ID'] == index2].values[0])
#        opportunityDf.loc[index2,'Name'] = check
#        opportunityDf.loc[index2,'type'] = 'Individual'
#        opportunityDf.loc[index2,'stage'] = 'Completed'
#        opportunityDf.loc[index2,'recordTypeId'] = '012A0000000GQ32IAG'
#        opportunityDf.loc[index2,'CloseDate'] = closeddate
#        opportunityDf.loc[index2,'Paid_by__c'] = contactData['Paid_by__c'][contactData['ID'] == index2].values[0]

    return contactIds[['Type','StageName','RecordTypeId','AccountId','npe01__Contact_Id_for_Role__c'\
                        ,'CloseDate' ,'Name','signup_id__c']]
    
    
    
#a = pd.read_csv(r"c:\temp\NWFB\downloads\contacts\contact.csv",sep=',',encoding='iso-8859-1')
#b = pd.read_csv(r"C:\temp\NWFB\uploads\contacts\contactsFile.csv",sep=',',encoding='iso-8859-1')
    
#c = opportunityFormat(a,b,'a')
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
