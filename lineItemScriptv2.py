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
productDetails = pd.read_csv(r"C:/temp/NWFB/dataDependencies/productDetails.csv",sep=',',encoding='iso-8859-1')


#def furniture_request_format(furniture_request,household_members,productDetails):
#    if household_members % 2 == 1:
#        number_of_chairs = household_members + 1
#    else:
#        number_of_chairs = household_members
#    pd.merge(furniture_request,productDetails[['Name','API_NAME','Product2Id','PricebookEntryId']],how='left',left_on='furniture_name',right_on='API_NAME')


def lineItemPrep(lineItemDF,productDetails,opportunityId):
#    household_members['number_of_chairs'] = ''
#    for index, item in household_members.iterrows():
#        if item['Total_Served__c'] % 2 == 1:
#            number_of_chairs = (int(item['Total_Served__c']) + 1)
#        else:
#            number_of_chairs = int(item['Total_Served__c'])
#        household_members.loc[index,'number_of_chairs'] = number_of_chairs
#    print(household_members)
#    print('prepping line items...')
    productList = []
    lineItemDF['opportunityId'] = ''
    for index, item in opportunityId.iterrows():
        lineItemDF.loc[index,'opportunityId'] = opportunityId.loc[index,'OPPORTUNITYID']
#    print("printing lineItemDF")    
#    print(lineItemDF)
    for index, item in lineItemDF.iterrows():
        print(index)
        print(item)
        for row in lineItemDF: 
            #Pivoting the dataframe so that each furniture record can be insert individually
            print(row)
            print(item[row])
            if str(row) == 'opportunityId':
                print('pass opp Id')
                pass
            elif str(row) == 'HomePhone':
                print('pass HomePhone')
                pass
            elif str(row) == 'USERID':
                print('pass USERID')
                pass
            elif (item[row] > 0) == True:
                productList.insert(0,[index,item['HomePhone'],lineItemDF.loc[index,'opportunityId'],row,item[row]]) # This line should be accepting the number of furniture line items requested....
                print(productList[0])
            else:
                pass
    furniture_request = df(columns = ['ID','HomePhone','opportunityId','furniture_item','number_requested'], data = productList)
    print("printing output")
    print(furniture_request)
#    for index, row in furniture_request.iterrows():
#        if row['furniture_item'] == 'DINETTE_SET_TABLE_CHAIRS':
#            furniture_request.loc[index,'number_requested'] = household_members['number_requested'][household_members['HomePhone'] == row['HomePhone']].values[0]
#        else:
#            pass
    return furniture_request

#furni_recs = pd.read_csv(r"C:\Users\marti\Desktop\test_db_output.csv",sep=',',encoding = 'iso-8859-1')
#furni_recs = furni_recs.set_index(furni_recs['HomePhone'])
#hm = df(columns = ['HomePhone','Total_Served__c'], data= [['123-456-7890',5],['222-333-4444',3]])
#oppid = df(columns = ['FORMATTEDPHONENUMBER__C','OPPORTUNITYID'], data= [['123-456-7890','oppId1'],['222-333-4444','oppId2']])
#prodslist = lineItemPrep(furni_recs,productDetails,oppid,hm)
    
#The below line of code needs to treat EVERY line item with a total greater than 1 like the previous dining room chairs
def lineitemprep2(request_list,product_details):
    output_list = pd.merge(request_list,product_details,how='inner',left_on = 'furniture_item',right_on='API_NAME')
#    multiple_items = []
#    for index, item in output_list.iterrows():
#        if item['Product2Id'] == '01tA0000002H5PkIAK':
#        for i in range(int(item['number_requested'])-1) :
#            multiple_items.insert(0,item)
#        print(multiple_items)
#    extra_chair_df = df(columns = output_list.columns, data = multiple_items)
#    tobejoined = [output_list, extra_chair_df]
#    output = pd.concat(tobejoined)
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