import os
import datetime as dt
import snowflake.connector
import urllib3
import json
import urllib.parse
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

legacy_org = os.environ["LEGACY_ORG"]

FOLDER_Library_Items = "01 Library Items"
FOLDER_Risk_Covers = "02 Risk Covers"
FOLDER_Comm_Internal = "03 Internal Communications"
FOLDER_Comm_External = "04 External Communications"
FOLDER_Comm_State_Trans = "05 Communication State Transitions"

starttime = dt.datetime.now()
dtstamp = starttime.strftime("%Y%m%d%H%M%S")


procon_user = os.environ['procon_user']
display_log = os.environ['display_log']

authenticatorval = os.environ['authenticatorval']
userval = os.environ['userval']
accountval = os.environ['accountval']
warehouseval = os.environ['warehouseval']
roleval = os.environ['roleval']
databaseval = os.environ['databaseval']
schemaval = os.environ['schemaval']
maxloop = int(os.environ['maxloop'])

domain = os.environ['domain']
target_folder = os.environ['target_folder']

# ORG specific parameters
if legacy_org == 'HWEL':
    load_dotenv(".env.hwel")
else:
    load_dotenv(".env.hbhp")

api_info = os.environ['api_info_file']
extract_type = os.environ['extract_type']
previous_extract_type = os.environ['previous_extract_type']
apihost = os.environ['apihost']
apiurl = os.environ['apiurl']
mytoken = os.environ['token']
export_dir = os.environ['export_directory']


os.environ['NO_PROXY'] = apihost
header = {'Authorization': "Bearer " + mytoken}
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

conn = snowflake.connector.connect(
    authenticator=authenticatorval,
    user=userval,
    account=accountval,
    warehouse=warehouseval,
    role=roleval,
    database=databaseval,
    schema=schemaval)
cur = conn.cursor()

DISPLAY_LOG_ON_CONSOLE = (display_log == "Yes")


def setup_log(log_file_name):
    logging.basicConfig(
        filename= "logs\\" + log_file_name, 
        encoding='utf-8', 
        format='%(asctime)s : %(message)s', 
        level=logging.INFO
        )
    logger = logging.getLogger(__name__)
    logger.info('Start of loading process')
    return logger


def insert_procon_log(p_log_name, p_log1, p_log2="", p_log3="", p_log4="", p_log5="" ):    
    p_log2 = "NULL" if p_log2 == "" else f"'{p_log2}'"
    p_log3 = "NULL" if p_log3 == "" else f"'{p_log3}'"
    p_log4 = "NULL" if p_log4 == "" else f"'{p_log4}'"
    p_log5 = "NULL" if p_log5 == "" else f"'{p_log5}'"
    sql_log_sf = f"""
        INSERT INTO PROCON_LOG (EXTRACT_TYPE, NAME, RUN_TIME, INFO1, INFO2, INFO3, INFO4, INFO5) 
        VALUES (
          '{extract_type}', '{p_log_name}', SYSDATE(), 
          '{p_log1}', {p_log2}, {p_log3}, {p_log4}, {p_log5} )
       """
    query_exec(sql_log_sf, __name__)


def insert_procon_extract(p_data, p_data_type, p_api_url, p_data_format, p_parent_link_id="" ):
    p_parent_link_id = "NULL" if p_parent_link_id == "" else f"'{p_parent_link_id}'"    
    sql_extract_sf = f"""
        INSERT INTO PROCON_EXTRACT (DATA, DATA_TYPE, API_URL, PARENT_LINKAGE_ID, DATA_FORMAT, INGEST_TIMESTAMP, EXTRACT_TYPE) 
            SELECT parse_json({p_data}),  
            '{p_data_type}', 
            '{p_api_url}', 
             {p_parent_link_id}, 
            '{p_data_format}', 
            SYSDATE(), '{extract_type}' 
        """
    query_exec(sql_extract_sf, __name__)


def update_procon_load(run_type, fileproconid, loadstatus, note="", usesystime=True):
    loadtime = 'LOADED_TIME = SYSDATE()'
    if not usesystime:
        loadtime = 'LOADED_TIME = NULL'

    sql_update_st = f"""
        UPDATE PROCON_LOAD_FILE SET FILE_LOADED = '{loadstatus}', NOTE = '{note}', {loadtime}  
        WHERE  extract_type = '{extract_type}' 
        AND FILEPROCONID = '{fileproconid}'  
        AND type = '{run_type}'
    """
    query_exec(sql_update_st, __name__)    


def query_exec(sql, func_name, to_list=False, params=()):
    the_list = []
    status = True
    if func_name == '__main__':
        func_name =''
    try:
        #print(f"\n{func_name}\n{sql}")
        #print('.', end='')
        cur.execute(sql, params)
        
        if to_list:
            results = cur.fetchall()
            for item in results:
                the_list.append(item[0])

    except Exception as e:
        status = False
        print(f"\n{func_name}\n{sql}\n\n{e}")

    return status, the_list 


def proc_response(response, api_url, datatype, offset, page, log_name, log4):
    next_page = False
    response_status = response.status_code
    url_parse = urllib.parse.quote(api_url)
    if response_status == 200:
        response_data = response.json()
        data_len = len(response_data)
        json_string = f"$${json.dumps(response_data)}$$"
        data_format = f"{str(type(response_data)).replace("'","")}"	
        if json_string.strip() != "$$[]$$":
            insert_procon_extract(json_string, datatype, api_url, data_format, log4)

        if( offset and (( data_len > 0 and page < maxloop) or (page==0)) ):
            page += 1
            next_page = True
    else:
        log3 = str(response.text).replace("'","\\'")
        insert_procon_log(log_name, url_parse, response_status, log3, log4, "FAILED")
    
    if DISPLAY_LOG_ON_CONSOLE:
        print(f"proc_response: {api_url}\n{response_status} | next_page:{next_page} | page:{page} | data_len:{data_len} | maxloop:{maxloop}")
    
    return response_status, next_page, page


def get_response(url_string, datatype, log_name, offset=False, page=0, log4=""):
    response=requests.get(url_string, headers=header, verify=False)
    status, next_page, page = proc_response(
        response, url_string, datatype, offset, page, log_name, log4)
    return status, next_page, page


def get_response_offset(base_url, datatype, log_name, contractref=''):
    new_results=True
    next_page=0
    url_string=base_url+"?offset="
    while new_results:
        url_run = url_string+"%s"%next_page
        status, new_results, next_page = get_response(url_run, datatype, log_name, offset=True, 
                                                      page=next_page, log4=contractref)


def get_project_response(base_url, l3level, project_numbers, datatype, log_name):
    for id in project_numbers:
        url_run = f"{base_url}{id}/{l3level}"
        get_response(url_run, datatype, log_name, log4=id )



def get_project_response_offset(base_url, l3level, project_numbers, datatype, log_name):
    for id in project_numbers:
        new_results=True
        next_page=0
        url_pass1 = base_url+"%s/%s?offset="%(id,l3level)
        while new_results:
            url_run=url_pass1+"%s"%next_page
            status, new_results, next_page = get_response(url_run, datatype, log_name, offset=True, page=next_page, log4=id)
            if status != 200:
                new_results = False


def get_lookup_response(base_url, l3level, project_numbers, datatype, log_name):    
    if l3level == 'project-dept-categories':
        for id in project_numbers:
            url_run = f"{base_url}{l3level}/{id}"
            get_response(url_run, datatype, log_name, log4=id)
    else:
        url_run = f"{base_url}{l3level}"
        get_response(url_run, datatype, log_name)


def get_users_response_offset(base_url, datatype, log_name):
    new_results=True
    next_page=0
    url_string = f"{base_url}?offset="
    while new_results:
        url_run = f"{url_string}{next_page}"
        status, new_results, next_page = get_response(url_run, datatype, log_name, offset=True, page=next_page)
        if status != 200:
            new_results = False


def request(session, id, url_api, datatype, log_name, offset):
    page = 0
    status = ''
    new_results = True
    log_count = 0
    while new_results:  
        url_string = url_api
        if offset:
            url_string = f"{url_api}{str(page)}"

        with session.get(url_string, headers=header, verify=False) as response:
            status, new_results, page = proc_response(
                        response, url_string, datatype, offset, page, log_name, id)

        if (((log_count < 10) or (status != 200)) and DISPLAY_LOG_ON_CONSOLE):
            log_text = f"{status} {url_string}"
            print(log_text)
        else:
            print('.', end='', flush=True)
        log_count += 1
