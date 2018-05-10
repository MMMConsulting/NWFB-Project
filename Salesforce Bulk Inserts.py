# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 22:15:09 2018

@author: micha
"""

from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
import unicodecsv
import time
from pandas import DataFrame as df

prod_bulk = SalesforceBulk(username='admin@havensconsulting.net',password='91dU9hsKZdkz',security_token='ATLclvPX1UFxT05UZMIHrAkM')
test_bulk = SalesforceBulk(username='michael@havensconsulting.net.mikesde',password='P@tersin1',sandbox=True, security_token='K7FjFIUW3KuMXKFthijArNDFP')

test_contacts = pd.read_csv(r"C:\Users\micha\Documents\NWFB\test_examples\contactsFile3.csv")

for column in test_contacts:
    for index, item in test_contacts.iterrows():
        test_contacts.loc[index,column] = str(test_contacts.loc[index,column]).replace('nan','')

job = test_bulk.create_insert_job("Contact", contentType='CSV', concurrency='Serial')

contacts = [dict(test_contacts) for idx in test_contacts]
contacts = contacts
#contacts = [dict(Name="Contact%d" % idx) for idx in range(5)]
csv_iter = CsvDictsAdapter(iter(test_contacts))
batch = test_bulk.post_batch(job, csv_iter)
test_bulk.batch_status()
test_bulk.wait_for_batch(job, batch)
test_bulk.close_job(job)
print("Done. Accounts uploaded.")
