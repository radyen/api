import requests 
import datetime as dt
import urllib.parse
import api_common as hc
import os
import time
import errno
import logging
import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor

# select one of these
comm_type_state_trans = False    # STATE TRANSITIONS
comm_type_external = True       # EXTERNAL COMM 
comm_type_internal = False      # INTERNAL COMM

warnings.filterwarnings('ignore')

print('Start of loading process')
header = hc.header
starttime = hc.starttime
dtstamp = starttime.strftime("%Y%m%d%H%M%S")

logging.basicConfig(
    filename=f'procon-load-comm-{dtstamp}.log', 
    encoding='utf-8', 
    format='%(asctime)s : %(message)s', 
    level=logging.INFO
    )
logger = logging.getLogger(__name__)
logger.info('Start of loading process')

# Internal comm is the default parameters
if comm_type_internal:
    file_type =  "legacy_communications_internal"
    log_name = "LCI share point load"
    comm_folder = hc.FOLDER_Comm_Internal

# External comm parameters
if comm_type_external:
    file_type = "legacy_communications_external"
    log_name = "LCE share point load"
    comm_folder = hc.FOLDER_Comm_External

if comm_type_state_trans:
    file_type = "communication_state_transitions"
    log_name = "Comm State Transitions share point load"    
    comm_folder = hc.FOLDER_Comm_State_Trans

doctype = comm_folder
directory= hc.export_dir
token_gen_time_min=30

def update_procon_load(fileproconid, loadstatus, note="", usesystime=True):
    loadtime = 'LOADED_TIME = NULL'
    if usesystime:
        loadtime = 'LOADED_TIME = SYSDATE()'

    sql_update_st = f"""
        UPDATE PROCON_LOAD_FILE SET FILE_LOADED = '{loadstatus}', NOTE = '{note}', {loadtime}  
        WHERE  extract_type = '{hc.extract_type}' 
        AND FILEPROCONID = '{fileproconid}'  
        AND type = '{file_type}'
    """
    hc.query_exec(sql_update_st, __name__)    


def request(session, url_api, fileproconid, filefullname, commproconref, contractproconref, log_name ):
    is_error = False
    error_text = ""
    url_string = f"{url_api}{fileproconid}/contents"
    with session.get(url_string, headers=header, verify=False) as response:
        response_status = response.status_code
        
        if response_status == 200:
            hc.insert_procon_log(
                log_name, 
                urllib.parse.quote(url_api), 
                response_status, "")
        else:
            update_procon_load(fileproconid, 'X', usesystime=False)

            hc.insert_procon_log(
                log_name, 
                urllib.parse.quote(url_api), 
                response_status, 
                response.text)
        
        final_filename = f"{commproconref}_{filefullname}"

        abs_path = os.path.join(directory,contractproconref,comm_folder, final_filename)

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

        except Exception as e:
            is_error = True
            update_procon_load(fileproconid, 'X', usesystime=False)
            error_text = str(e)

        result_text = f"{response_status} | {fileproconid} | {abs_path}"
        if is_error:
            logger.info(f"\nFAILED: {result_text}\n{error_text}")
        else:                
            logger.info(f"{result_text}")


async def get_api_response(base_url, selected_comms, logname):
    col_FILEPROCONID = 0
    col_FILEFULLNAME = 1
    col_COMMUNICATIONPROCONREFERENCE = 2
    col_CONTRACTPROCONREFERENCE = 3

    index_list = range(len(selected_comms))
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            for i in index_list:
                fileproconid = selected_comms[i][col_FILEPROCONID]
                filefullname = selected_comms[i][col_FILEFULLNAME]
                commproconref = selected_comms[i][col_COMMUNICATIONPROCONREFERENCE]
                contractproconref = selected_comms[i][col_CONTRACTPROCONREFERENCE]
                loop.run_in_executor(executor, 
                                     request, 
                                     *(session, 
                                       base_url, 
                                       fileproconid, 
                                       filefullname, 
                                       commproconref, 
                                       contractproconref, 
                                       logname) 
                                       )



try:

    insert_st = f"""
        INSERT INTO PROCON_LOAD_FILE (FILEPROCONId, file_loaded, is_active, type, extract_type)  
        SELECT DISTINCT FILEPROCONID , 'N' file_loaded,'Y' is_active,'{file_type}' type, extract_type 
        FROM PROCON_DOCUMENTS_VW 
        WHERE extract_type = '{hc.extract_type}' 
        AND DOCUMENT_TYPE IN  ('{doctype}')
        MINUS   
        SELECT DISTINCT  FILEPROCONID, 'N','Y', TYPE, extract_type  
        FROM  PROCON_LOAD_FILE  
        WHERE type = '{file_type}'  
        AND extract_type = '{hc.extract_type}' 
    """
    hc.cur.execute(insert_st)
    #print(insert_st)

    # to speed up the process, exclude files that have been downloaded in previous extract (RE 9/10/24)
    bulk_update_load_in_prev_extract = f"""
        UPDATE PROCON_LOAD_FILE 
        SET FILE_LOADED = 'Y', LOADED_TIME = SYSDATE()
        WHERE  extract_type = '{hc.extract_type}' 
        AND type = '{file_type}'
        AND FILEPROCONID IN (
            SELECT DISTINCT FILEPROCONID
            FROM PROCON_DOCUMENTS_VW
            WHERE EXTRACT_TYPE LIKE '%{hc.extract_type[4:8]}%'  
            AND DOCUMENT_TYPE IN ('{doctype}')          
            MINUS
            SELECT DISTINCT FILEPROCONID
            FROM PROCON_DOCUMENTS_VW
            WHERE EXTRACT_TYPE = '{hc.extract_type}'  
            AND DOCUMENT_TYPE IN ('{doctype}')                         
        )     
    """         
    hc.cur.execute(bulk_update_load_in_prev_extract)
    #print(bulk_update_load_in_prev_extract)

    sql_file_info = f"""
        SELECT DISTINCT FILEPROCONID, FILEFULLNAME, COMMUNICATIONPROCONREFERENCE, CONTRACTPROCONREFERENCE  
        FROM ( 
           SELECT a.* FROM clean_current_procon_communications_attachments a 
           JOIN PROCON_LOAD_FILE b 
            ON a.FILEPROCONId=b.FILEPROCONId 
            AND is_active = 'Y' 
            AND file_loaded = 'N' 
            AND type = '{file_type}' 
            AND b.extract_type = a.extract_type 
        ) att
        JOIN clean_current_procon_post_awards_communications comm 
            ON att.parentproconid = comm.COMMUNICATIONPROCONID 
            AND att.extract_type = comm.extract_type 
            AND att.extract_type = '{hc.extract_type}' 
            AND to_number(att.fileproconid) between 0 and 999999999999999 
        ORDER BY to_number(att.fileproconid) 
    """          
    
    sql_comm_state_trans = f"""
        SELECT FILEPROCONID, ORIGINAL_FILENAME AS FILEFULLNAME,
        FILEPROCONID AS COMMUNICATIONPROCONREFERENCE, 
        CONTRACTPROCONREFERENCE
        FROM PROCON_DOCUMENTS_VW
        WHERE EXTRACT_TYPE = '{hc.extract_type}'
        AND DOCUMENT_TYPE = '{doctype}'
    """

    if comm_type_state_trans:
        sql_file_info = sql_comm_state_trans
    
    results_file_info = hc.cur.execute(sql_file_info).fetchall()
    #print(sql_file_info)

    session_time = dt.datetime.now()
    time_change = dt.timedelta(minutes=token_gen_time_min)
    time.sleep(2)

    # only download files that have not beed downloaded in previous extract
    base_url = f"https://{hc.apihost}/proconRestApi/procon/v1/files/"
    
    print(f"Number of attachment files to be extracted: {len(results_file_info)}")

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_api_response(base_url, results_file_info, log_name))
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

