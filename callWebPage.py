# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 15:49:37 2017

@author: marti

This script will pull the data from the server download folder
"""
import pandas as pd
import time 
import datetime
#import os

allCon = r"C:\temp\NWFB\sampleData\contactData.csv"
janeDoe = r"C:\temp\NWFB\sampleData\janeDoe.csv"

'''Need link for account/agency lookup'''
'''Need link for caseworker lookup form'''


'''
def findTheFile:

   This function will ultimately grab the file from the real location based on time of day 
   return path
'''    

'''
set the dtypes for the data being imported...
'''

def importData(path,filetype):
    '''
    I take the path and pull the data into memory
    '''
    downloadTime = time.time()
    downloadTime = datetime.datetime.fromtimestamp(downloadTime).strftime('%Y-%m-%d-%H.%M.%S')
    if filetype == 'csv':
        output = pd.read_csv(path,encoding = 'iso-8859-1')
    else:
        'read text in some manner'
    output['caseworkerId'] = ''
    output['sfaccntid'] = ''
    return output, downloadTime

#def lookups(dataframe):
#    caseWorkerLU
#    agencyLU
    
    
    
    
    
    
    
    


#output,downloadTime = importdata(janeDoe,"csv")
