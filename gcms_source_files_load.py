#import snowflake.connector
import pandas as pd
#import os
#import urllib3
import datetime
from snowflake.connector.pandas_tools import write_pandas
import csv
from os import walk

import hbhp_common as hc

###PLEASE SET UP EXTRACT_TYPE

extract_type = 'PH1_GCMS'

###this should be YES only for 1st time when we want to create tables from scratch**********************************
is_initial_load = 'NO'


start_time = str(datetime.datetime.now())
print (start_time)
#*****************************************************************************************************************

##don't change this in normal condition make sure this is always yes, so duplicate data don't created
is_clean_up_required = 'YES'
###Printing just to make sure we wanted initial load
if(is_initial_load=='YES'):
    print('THIS IS INITIAL LOAD****************************************')
    print('*************THIS IS INITIAL LOAD******************')
    print('***************THIS IS INITIAL LOAD**************')
    print('****THIS IS INITIAL LOAD***********************')

print('start of loading process')
#Getting connection info
#from connection_info import (apihost,apiurl,authenticatorval,userval,accountval,warehouseval,roleval,databaseval,schemaval,myToken, maxloop)
#os.environ['NO_PROXY'] = apihost
#header = {'Authorization': "Bearer " + myToken}


#from datetime import datetime
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#conn = snowflake.connector.connect(authenticator=authenticatorval,user =userval,account=accountval,warehouse=warehouseval,role=roleval,database=databaseval,schema=schemaval)
#cur = conn.cursor()
conn = hc.conn 
cur = hc.cur

#source_dir = r"C:\Temp\AWS\jaggaer\GCMS-Extract-20241031\manifest"
source_dir = r"C:\Temp\AWS\jaggaer\PH0_GCMS"

try:
    filenames = next(walk(source_dir), (None, None, []))[2]  # [] if no file
    print('****filenames:')
    print(filenames)
    print('****total availalbe files')
    print(len(filenames))

    for i in range(len(filenames)):
        try:
            print(filenames[i])

            file_name = filenames[i]
            table_name = 'GCMS_'+file_name.replace('.csv','')
            table_name = table_name.upper()
            print(table_name)
            
            with open(source_dir+'\\'+file_name,encoding="utf8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter = ',')
                list_of_column_names = []
                # loop to iterate through the rows of csv
                for row in csv_reader:
                    list_of_column_names.append(row)
                    break
            

            ddl_script = str(list_of_column_names[0]).replace('[','create or replace table '+table_name+' (').replace(']','  varchar(2000)**addedcol)').replace(',',' varchar(2000),').replace("'","").replace("**addedcol",',INGEST_TIMESTAMP TIMESTAMP_TZ(9) DEFAULT SYSDATE(),EXTRACT_TYPE varchar(100) ')
            print("List of column names : ",
                str(list_of_column_names[0]))
            print('*******ddl_script:')
            print(ddl_script)

            #if ddl need to run
            if(is_initial_load == 'YES'):
                print('*****executing  ddl')
                cur.execute(ddl_script)

            #Cleanup required
            if(is_clean_up_required =='YES'):
                cleanup_script='delete from '+table_name+' where extract_type is null OR EXTRACT_TYPE = '+"'"+extract_type+"'"
                print('******executing cleanup script')
                print(cleanup_script)
                cur.execute(cleanup_script)

            '''  temp
            ####reading and loading file
            print('*****reading csv****')
            original = source_dir+"\\"+file_name # <- Replace with your path.
            delimiter = "," # Replace if you're using a different delimiter.
            #total = pd.read_csv(original, sep = delimiter)
            #total = pd.read_csv(original,dtype=str, sep = delimiter, on_bad_lines = 'warn').fillna('')
            total = pd.read_csv(original,dtype=str, sep = delimiter, on_bad_lines = 'warn')
            #total =total.astype(str) ##this was removed as it caused null to convert into nan
            # Actually write to the table in snowflake.
            print('**** loading csv started***')
            write_pandas(conn, total, table_name)
            update_extract_type_script = "update "+table_name+" set extract_type ='"+extract_type+"' where extract_type is null"
            print('executing update extract type script')
            print(update_extract_type_script)
            cur.execute(update_extract_type_script)

            log_st = "INSERT INTO GCMS_LOG (NAME,RUN_TIME,INFO1,INFO2,INFO3,INFO4) VALUES ('GCMS source files snowflake load',SYSDATE(),'"+file_name+"','"+table_name+"',"+"'LOADED','"+start_time+"')"
            print(log_st)
            cur.execute(log_st)
            print('****loading csv completed****')

            '''

        except Exception as err:
            log_st = "INSERT INTO GCMS_LOG (NAME,RUN_TIME,INFO1,INFO2,INFO3,INFO4) VALUES ('GCMS source files snowflake load',SYSDATE(),'"+file_name+"','"+table_name+"',"+"'FAILED','"+start_time+"')"
            print(log_st)
            #cur.execute(log_st)
            print('******error')
            print(err)
            print('*********update log')
            update_log_st = "update GCMS_LOG set INFO5 ='"+str(err).replace("'","\\'")+"' where name = 'GCMS source files snowflake load' and info1 ='"+file_name+"' and info4 = '"+start_time+"'"
            print(update_log_st)
            #cur.execute(update_log_st)
            print("Something went wrong: " + str(err))
    
finally:
    cur.close()  
conn.close
if(is_initial_load=='YES'):
    print('THIS IS INITIAL LOAD')


