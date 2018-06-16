# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 20:06:14 2017

@author: marti

This script pulls todays contacts, matches their names in order to use the contact ID, then appends lineitems to them 
and uploads them into SF
"""

import pandas as pd
from pandas import DataFrame as df
import os

opDir = 'C:/Temp/NWFB//salesforce.com/Data Loader/bin/'
productDetails = pd.read_csv(r"C:/temp/NWFB/dataDependencies/productDetails_2.csv",sep=',',encoding='iso-8859-1')



def lineItemPrep(lineItemDF,productDetails,opportunityId):
    lineItemDF['opportunityId'] = ''
    opportunityId.index = opportunityId.index.map(str)
    lineItemDF.index = lineItemDF.index.map(str)
    pre_output = pd.merge(lineItemDF\
                          , opportunityId[['Id','CreatedDate']]\
                          , how='inner'\
                          , left_index=True\
                          , right_index=True\
                          )
    furniture_request = pre_output
    print("printing output")
    print(furniture_request)
    return furniture_request


#The below line of code needs to treat EVERY line item with a total greater than 1 like the previous dining room chairs
def lineitemprep2(request_list,product_details):
    output_list = pd.merge(request_list,product_details,how='inner',left_on = 'CODE',right_on='Code')
    output_list['Declined__c'] = 0
    output_list['N_A__c'] = 0
    for index, row in output_list.iterrows():
        if row['STATUS'] == 'Declined':
            output_list.loc[index,'Declined__c'] = 1
        elif row['STATUS'] == 'Not Available':
            output_list.loc[index,'N_A__c'] = 1
        else:
            pass
    '''
    This part of the code is comparing the ProductId to the PricebookEntry Id
    The reason version will likely look up the product names against this table
    '''
    print('preparing line items for upload...')
    return output_list

#output = lineitemprep2(prodlist, productDetails)

#def lineItemUpload(directory):
#    print("Uploading LineItems...")
#    os.chdir(directory)
#    output = os.system('process ../samples/conf/ contactInsertProcess')
#   print(output)
#    print("...Closing Upload")

#contactIds = pd.read_csv(r"c:\temp\NWFB\downloads\contacts\contact.csv",sep=',',encoding = 'iso-8859-1')

#linetest = pd.read
#opportunityId = pd.read_csv(r"C:\temp\NWFB\downloads\opportunity\opportunity.csv",sep=',',encoding='iso-8859-1')

#a = lineItemPrep(linetest,productDetails,opportunityId)

#print(a)