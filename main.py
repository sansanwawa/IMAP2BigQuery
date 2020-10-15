# File name: main.py
# Author: Sandy Haryono <sandyharyono@gmail.com>
# Python Version: 3.7.5

#import google cloud lib
from google.cloud.bigquery.client import Client
from google.cloud import bigquery

import imaplib
import pprint
import email
import os
import base64
import csv
import json
import sys

#Software Info
SOFTWARENAME                    = 'IMAP to BigQuery Importer'
VERSION                         = 'v.1'

#BigQuery Configuration
PROJECT_ID                      = 'Your Project Id'
DATASET_ID                      = 'Your Data Set Id'
SECRET_SERVICE_ACCOUNT_KEY_FILE = 'Your Json Key.json'   

#Email configurations
IMAP_HOST                       = 'imap.gmail.com'
IMAP_USER                       = 'Your username of email'
IMAP_PASSWORD                   = 'Your password of email'
IMAP_PORT                       = 993






class IMAPProcess(object):

    def __init__(self):
        # connect to host using SSL
        self.__imap = imaplib.IMAP4_SSL(IMAP_HOST,IMAP_PORT)

    def login(self):
         ## login to server
        self.__imap.login(IMAP_USER, IMAP_PASSWORD)

    def select(self):
        self.__imap.select('Inbox')
    
    def close(self):
        self.__imap.close()
    
             
    def fetch(self):
        
        tmp, data = self.__imap.search(None, '(UNSEEN)') 
        for num in data[0].split():
            tmp, data = self.__imap.fetch(num, '(RFC822)')
       
            raw_email = data[0][1]
            raw_email_string = raw_email.decode('utf-8')
            email_message = email.message_from_string(raw_email_string)
            subject = email_message['subject']
            if '|' in subject and '-' in subject:
                subjects = subject.split('|')
                subsubject01        = subjects[0].split('-')
                subsubject02        = subjects[1].split('-')
                tableName           = subsubject01[1].strip()
                headerRowNumber     = int(subsubject02[0].strip())
            elif '|' not in subject and '-' in subject:
                subsubject          = subject.split('-')
                tableName           = subsubject[1].strip()
                headerRowNumber     = 1
                





            # downloading attachments
            for part in email_message.walk():
                # this part comes from the snipped I don't understand yet... 
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                fileName = part.get_filename()
             
                if bool(fileName):
                    filePath = os.path.join('/tmp/', fileName)
                    if not os.path.isfile(filePath) :
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
                    subject = str(email_message).split("Subject: ", 1)[1].split("\nTo:", 1)[0]
                    print('Downloaded "{file}" from email titled "{subject}" with UID {uid}.'.
                    format(file=fileName, subject=subject, uid=num))
    
                    csvObj      = CSVFile(filePath,headerRowNumber)
                    if csvObj.run():
                        schema      = csvObj.getSchema()
                        data        = csvObj.getData()
                        BigQueryPlugIn(tableName,schema).createTable()    
                        BigQueryPlugIn(tableName,schema).insert(data)    
                    
            #self.__imap.store( num, '-FLAGS', '\\Seen')
           
    def execute(self):
        self.login()
        self.select()
        self.fetch()
        self.close()









class CSVFile(object):

    def __init__(self,file, headerRow):
        self.__file = file
        self.table_name = 'csvfile'
        self.__schema   = ''
        self.__data     = ''
        if headerRow == 0:
            self.__headerRow = 1
        else:
            self.__headerRow = headerRow - 1

    def run(self):
        with open(self.__file, 'r') as file:
            reader = csv.reader(file)
            i = 0
            data = []
            for row in reader:
                if i == self.__headerRow:
                   self.__schema = row
                elif i > self.__headerRow:
                   data.append(row)
                i += 1

            header = self.__schema
    
            jsonRow ='['
            y=0
            for d in data:
                x=0
                subJson = '{'
                for s in d:
                    subJson += '"{}":"{}",'.format(header[x], s) 
                    x+=1
                jsonRow += subJson[:-1] + '},'
                if y==2:
                    break
            y+=1
            jsonRow     = jsonRow[:-1] + ']'
            json_data   = json.loads(jsonRow)
            self.__data = json_data 
            
            print('--------------------------')
        return True

    def getSchema(self):
        return self.__schema

    def getData(self):
        return self.__data

class BigQueryPlugIn(object):
     
   
    #Constructor
    def __init__(self,TABLE_ID,SCHEMA):
        self.__client   = Client.from_service_account_json(SECRET_SERVICE_ACCOUNT_KEY_FILE, project = PROJECT_ID)
        self.__schema   = SCHEMA
        self.__table_id = '{}.{}.{}' .format(PROJECT_ID,DATASET_ID,TABLE_ID)
        print("Checking a Table Id : " + self.__table_id)
       
    
    def createTable(self):
        #Check if there is already table or not
        #if there no table exists, then not required to create the table
            if self.isTableExist() == False:
                #preparing the schema  
                schema = []
                for sch in self.__schema:
                    schema.append(bigquery.SchemaField(sch,'STRING'))
                #sys.exit()

                table = bigquery.Table(self.__table_id, schema=schema)
                table = self.__client.create_table(table) 
                print("Creating a Table Id {}".format(self.__table_id))
            else:
                print("Table {} exists! No need to create it...".format(self.__table_id))
            

    def isTableExist(self):
        try:
            self.__client.get_table(self.__table_id)
            return True
        except:
            return False
    
    def insert(self,rows_to_insert):
        print('Processing {} data to be inserted'.format(str(len(rows_to_insert))) )
        if len(rows_to_insert) > 0:
            errors = self.__client.insert_rows_json(self.__table_id, rows_to_insert)  # Make an API request.
            if errors == []:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))

    def delete(self):        
        d = 'DELETE FROM {} WHERE true'.format(self.__table_id)
        q = self.__client.query(d)
        q.result()



if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    IMAPProcess().execute()
