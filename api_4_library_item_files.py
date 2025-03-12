import requests 
import datetime as dt
import time
import urllib.parse
import api_common as hc
import os
import errno
import logging
import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

base_url = f"https://{hc.apihost}/proconRestApi/procon/v1/files/" 
log_name_li = 'Library Items files extract'
log_name_rc = 'Risk Cover files extract'

token_gen_time_min=30
col_FILEPROCONID = 0
col_CONTRACTPROCONREFERENCE = 1
col_FILEFULLNAME = 2
col_DATATYPE = 3
directory= hc.export_dir

print('Start of loading process')
header = hc.header
starttime = hc.starttime
dtstamp = starttime.strftime("%Y%m%d%H%M%S")
logging.basicConfig(
    filename=f'procon-hbhp-load-files-{dtstamp}.log', 
    encoding='utf-8', 
    format='%(asctime)s : %(message)s', 
    level=logging.INFO
    )
logger = logging.getLogger(__name__)
logger.info('Start of loading process')

doctype_li = hc.FOLDER_Library_Items
doctype_rc = hc.FOLDER_Risk_Covers

def update_procon_load(fileproconid, loadstatus, usesystime=True):
    if usesystime:
        loadtime = 'LOADED_TIME = SYSDATE()'
    else:
        loadtime = 'LOADED_TIME = NULL'

    sql_update_st = f"""
        UPDATE PROCON_LOAD_FILE set FILE_LOADED = '{loadstatus}', {loadtime}  
        WHERE  extract_type = '{hc.extract_type}' 
        AND FILEPROCONID = '{str(fileproconid)}'  
        AND type IN ('post-awards/library-items', 'post_awards/risk_cover_files') 
    """
    hc.query_exec(sql_update_st, __name__)    


def request(session, fileproconid, url_api, filefullname, contractproconref, datatype, log_name):
    url_parse = urllib.parse.quote(url_api)
    log_text = ""
    final_filename = f"{fileproconid}-{filefullname}"
    folder_name = hc.FOLDER_Library_Items
    if datatype == "post_awards/risk_cover_files":
        folder_name = hc.FOLDER_Risk_Covers
    

    abs_path = os.path.join(directory, contractproconref, folder_name, final_filename)

    with session.get(url_api, headers=header, verify=False) as response:
        response_status = response.status_code
        if response_status == 200:
            hc.insert_procon_log(log_name, url_parse, str(response.status_code) )  
        else:
            update_procon_load(fileproconid, 'X', usesystime=False)                
            hc.insert_procon_log(log_name, url_parse, str(response.status_code), str(response.text) ) 
        try:
            if not os.path.exists(os.path.dirname(abs_path)):
                try:
                    os.makedirs(os.path.dirname(abs_path))
                except OSError as exc: # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise
                
            f=open(abs_path,'wb')
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
            f.close()
            update_procon_load(fileproconid, 'Y')
            log_text = f"{response_status}: {abs_path}"

        except Exception as e:
            update_procon_load(fileproconid, 'X', usesystime=False)   
            print(e)   
            log_text = f"{response_status}: {abs_path}\n{e}"
        
        logger.info(log_text)



async def get_api_response(base_url, results_file):
    input_list = range(len(results_file))
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            for id in input_list:
                fileproconid = results_file[id][col_FILEPROCONID]  
                contractproconref = results_file[id][col_CONTRACTPROCONREFERENCE]
                filefullname = results_file[id][col_FILEFULLNAME]
                datatype = results_file[id][col_DATATYPE]
                
                log_name = log_name_li
                if datatype == 'post_awards/risk_cover_files':
                    log_name = log_name_rc

                print(f"{contractproconref} | {fileproconid} | {filefullname}")

                api_url = f"{base_url}{fileproconid}/contents"

                loop.run_in_executor(
                    executor, request, *(
                        session, fileproconid, api_url,filefullname, 
                        contractproconref, datatype, log_name) 
                        )


                
try:
    ##file_export root location
    #print('dir:'+ directory)
   
    insert_st = f"""
        INSERT INTO PROCON_LOAD_FILE (FILEPROCONId,file_loaded,is_active,type,extract_type) 
          SELECT DISTINCT FILEPROCONID, 'N' file_loaded,'Y' is_active,
          CASE WHEN DOCUMENT_TYPE = '{doctype_li}' THEN 'post-awards/library-items'
          	WHEN DOCUMENT_TYPE = '{doctype_rc}' THEN 'post_awards/risk_cover_files'
          END AS type, extract_type 
          FROM PROCON_DOCUMENTS_VW 
          WHERE extract_type = '{hc.extract_type}' 
          AND DOCUMENT_TYPE IN  ('{doctype_li}', '{doctype_rc}')
		  MINUS  
          SELECT DISTINCT  FILEPROCONId, 'N','Y', TYPE, extract_type 
          FROM  PROCON_LOAD_FILE 
          WHERE extract_type = '{hc.extract_type}'
          AND type IN ('post-awards/library-items', 'post_awards/risk_cover_files')  
           
    """
    #print(insert_st)
    hc.cur.execute(insert_st)


    sql_bulk_update_st = f"""
        UPDATE PROCON_LOAD_FILE 
        SET FILE_LOADED = 'Y', LOADED_TIME = SYSDATE()
        WHERE  extract_type = '{hc.extract_type}' 
        AND FILEPROCONID IN (
            SELECT DISTINCT FILEPROCONID
            FROM PROCON_DOCUMENTS_VW
            WHERE EXTRACT_TYPE LIKE '%{hc.extract_type[4:8]}%'  
            AND DOCUMENT_TYPE IN ('{doctype_li}', '{doctype_rc}')          
            MINUS
            SELECT DISTINCT FILEPROCONID
            FROM PROCON_DOCUMENTS_VW
            WHERE EXTRACT_TYPE = '{hc.extract_type}'  
            AND DOCUMENT_TYPE IN ('{doctype_li}', '{doctype_rc}')                
        )
    """
    #print(sql_bulk_update_st)
    hc.query_exec(sql_bulk_update_st, __name__)    

    select_file_info = f"""
         SELECT DISTINCT a.FILEPROCONID, CONTRACTPROCONREFERENCE, a.ORIGINAL_FILENAME, b.TYPE 
         FROM PROCON_DOCUMENTS_VW a, PROCON_LOAD_FILE b  
         WHERE a.FILEPROCONID = b.FILEPROCONID  
         AND a.DOCUMENT_TYPE IN  ('{doctype_li}', '{doctype_rc}')
         AND b.extract_type = a.extract_type 
         AND a.extract_type = '{hc.extract_type}'  
         AND is_active = 'Y'  
         AND file_loaded = 'N' 
         AND to_number(a.fileproconid) between 0 AND 999999999999999  
         ORDER BY to_number(a.fileproconid)

    """
    results_file_info = hc.cur.execute(select_file_info).fetchall()
    #print(select_file_info)

    count_new_files = len(results_file_info)
    if count_new_files == 0:
        print("No new file.")
    else:
        print(f"{count_new_files} to be extracted via API.")


    session_time = dt.datetime.now()
    time_change = dt.timedelta(minutes=token_gen_time_min)
    time.sleep(2)
    
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_api_response(base_url, results_file_info))
    loop.run_until_complete(future)


finally:
    endtime = dt.datetime.now()
    log_start = f'Started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}'
    log_finish = f'Finished at: {endtime.strftime("%Y-%m-%d %H:%M:%S")}'
    log_total = f'Total time {endtime-starttime}'
    log_elapse_time = f'\n{log_start}\n{log_finish}\n{log_total}'  
    print(f'\n{log_elapse_time}')
    logger.info(f'{log_elapse_time}\n\nEnd of loading process')

    hc.cur.close()  
        
hc.conn.close

