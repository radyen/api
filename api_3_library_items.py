import datetime as dt
import api_common as hc
import pandas as pd
import logging
import asyncio
import warnings
import requests
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

col_CONTRACTPROCONREFERENCE = 0
col_PROCONID = 1

print('Start of loading process')
header = hc.header
starttime = hc.starttime
dtstamp = starttime.strftime("%Y%m%d%H%M%S")
logging.basicConfig(
    filename=f'procon-{hc.legacy_org.lower()}-load-library-items-{dtstamp}.log', 
    encoding='utf-8', 
    format='%(asctime)s : %(message)s', 
    level=logging.INFO
    )
logger = logging.getLogger(__name__)
logger.info('Start of loading process')



async def get_api_response(base_url, type, id_list, datatype, logname, offset):
    print(f"{datatype}")
    is_library_item = False
    is_risk_file = False
    if datatype == 'post-awards/library-items':
        is_library_item = True
        base_api_url = f"{base_url}?offset="

    if datatype == 'post_awards/risk_cover_files':
        is_risk_file = True

    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            for id in range(len(id_list)):
                api_url = ""
                if is_library_item:
                    proconid = id_list[id][col_PROCONID]
                    ref = id_list[id][col_CONTRACTPROCONREFERENCE]
                    api_url = base_api_url.replace("{proconid}",str(proconid) )
                if is_risk_file:
                    api_url = id_list[id]
                    ref = id   
                loop.run_in_executor(executor, hc.request, *(session, ref, api_url, datatype, logname, offset) )




#get api list
api_sheet_name = 'API-DOCS'
apilist = pd.read_excel(hc.api_info,sheet_name=api_sheet_name)
apilistpostawardslibraryitem=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'LIBRARYITEMS')] 
riskcoverfiles=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'RISKCOVERFILES')] 

err_log_txt = """
'{"message":"Reopen Contract - you cannot perform this action. You may not be authorised.","statusCode":403,"details":["Reopen Contract - you cannot perform this action. You may not be authorised."]}', 
'{"message":"You cannot close this contract as it currently has active alerts.","statusCode":404,"errorNumber":60140,"details":["You cannot close this contract as it currently has active alerts."]}' 
"""

select_sql = f"""
SELECT CONTRACTPROCONREFERENCE, PROCONID 
FROM ( 
    SELECT CONTRACTPROCONREFERENCE, proConId FROM clean_current_procon_organisation_post_awards 
    WHERE EXTRACT_TYPE = '{hc.extract_type}' 
    UNION  
    SELECT CONTRACTPROCONREFERENCE,proConId FROM clean_current_procon_Project_depts_Post_Awards  
    WHERE EXTRACT_TYPE = '{hc.extract_type}'   
) WHERE PROCONID IN (  
    SELECT PROCONID  
    FROM PROCON_LOAD_PROCONID  
    WHERE is_active = 'Y' 
    AND EXTRACT_TYPE = '{hc.extract_type}') 
    AND PROCONID not IN ( 
        SELECT DISTINCT info4 FROM procon_log 
        WHERE name like 'Add access to%'  
        AND EXTRACT_TYPE = '{hc.extract_type}'  
        AND info3 IN ( {err_log_txt} ) 
) 
 --AND CONTRACTPROCONREFERENCE = 'CONTRACT NO. 4700001521 - TRION - FPU DRY TRANSPORTATION'  
ORDER BY to_number(proconid) 
"""

try:  
    ####loading LIBRARY ITEMS for open contract and exluding proconid which had issues while getting access.
    log_name = 'Library Items snowflake load'
    li_results = hc.cur.execute(select_sql).fetchall()

    for api in apilistpostawardslibraryitem.API:
        #print(f"\nAPI: {apiurl}")
        datatype = 'post-awards/library-items'
        api_url =  f"{hc.apiurl}{api}"
        print(f"\nAPI: {api_url}")
        #hc.get_response_offset(url_string, datatype, log_name, contractref=contractproconreference)
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(get_api_response(api_url, '', li_results, datatype, log_name, True))
        loop.run_until_complete(future)


    #### loading RISK COVERS files
    sel_file_url = f"""
    SELECT DISTINCT CERTIFICATEFILEURL  
    FROM CLEAN_CURRENT_PROCON_POST_AWARDS_RISK_COVERS  
    WHERE EXTRACT_TYPE = '{hc.extract_type}' 
    AND CERTIFICATEFILEURL IS NOT NULL 
    """ 
    out, risk_cover_file_list = hc.query_exec(sel_file_url, __name__, to_list=True)

    log_name = 'Risk cover files snowflake load'
    for apiurl in riskcoverfiles.API:
        print(f"\nAPI: {apiurl}")
        datatype = 'post_awards/risk_cover_files'
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(get_api_response('', '', risk_cover_file_list, datatype, log_name, False))
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


