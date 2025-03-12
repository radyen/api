import requests 
import pandas as pd
import datetime as dt
import urllib.parse
import api_common as hc
import json
import asyncio
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')


starttime = dt.datetime.now()
dtstamp = starttime.strftime("%Y%m%d%H%M%S")

logging.basicConfig(
    filename=fr'procon-{hc.legacy_org.lower()}-tenders-{dtstamp}.log', 
    encoding='utf-8', 
    format='%(asctime)s : %(message)s', 
    level=logging.INFO
    )
logger = logging.getLogger(__name__)
logger.info('Start of loading process')

header = hc.header
conn = hc.conn
cur = hc.cur
maxloop = hc.maxloop
extract_type = hc.extract_type



async def get_api_response(base_url, type, id_list, datatype, logname, offset=True):
    print(f"\nDATA TYPE: {datatype}")
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            for id in id_list:
                api_url = ""
                str_id = str(id)
                if offset:
                        api_url = f"{base_url}{id}/{type}?offset="
                        if (len(type)==0 and len(id) == 0):
                            api_url = f"{base_url}?offset="
                        elif (len(type)==0 and len(id) > 0):
                            api_url = base_url.replace("{id}",str_id ) + "?offset="
                else:
                    api_url = base_url.replace("{id}",str_id )
                
                loop.run_in_executor(executor, hc.request, *(session, id, api_url, datatype, logname, offset) )



if __name__ == '__main__':
    print('Start of loading process')
    log_name = 'Tenders snowflake load'
    api_sheet_name = 'API-TENDERS'

    #get api list
    apilist = pd.read_excel(hc.api_info,sheet_name=api_sheet_name)

    try:
        project_list_sql = f"""
            SELECT DISTINCT tier3ProConId PROJECT_LIST 
            FROM clean_current_procon_organisation_project_depts 
            WHERE extract_type = '{extract_type}' 
        """
        #print(project_list_sql)
        out, project_list = hc.query_exec(project_list_sql, __name__, to_list=True)

        #####data load #####################################################################################################
        
        #tenders
        datatype = 'project-depts/tenders'
        tender_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in tender_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', project_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        #get tender list
        tender_list_sql = f"""
            SELECT DISTINCT tenderproconid 
            FROM clean_current_procon_tenders_project_depts_tenders 
            WHERE extract_type = '{extract_type}' """
        
        out, tender_list = hc.query_exec(tender_list_sql, __name__, to_list=True)
        
        print(f"\n{tender_list}")
        datatype = 'tenders/schedule_items'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tender_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        
        #tenders 'tenders_bidders','tenders/submission_files','tenders/tender_messages','tenders/schedule_items'
        datatype = 'tenders/bidders'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tender_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)
        
        datatype = 'tenders/submission_files'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tender_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        datatype = 'tenders/tender_messages'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tender_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)


        datatype = 'tenders/files'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tender_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)


        ###'tenders/tender_messages_files'
        tendermessageproconid_list_sql = f"""
            SELECT tendermessageproconid 
            FROM clean_current_procon_tenders_tender_messages 
            WHERE extract_type = '{extract_type}' 
            AND HASATTACHMENTS = 'true' 
            AND to_number(tendermessageproconid) BETWEEN  0 AND	999999999999999
            AND tendermessageproconid not in (
                SELECT split_part(info1,'/',8) 
                FROM procon_log a 
                WHERE extract_type = '{extract_type}' 
                AND info2 = 200 
                AND name = 'tenders/tender_messages_files snowflake load')
            ORDER BY to_number(tendermessageproconid)"""

        out, tendermessageproconid_list = hc.query_exec(tendermessageproconid_list_sql, __name__, to_list=True)

        #print(tendermessageproconid_list)

        datatype = 'tenders/tender_messages_files'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tendermessageproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        ###'tenders/team_members'
        datatype = 'tenders/team_members'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', tender_list, datatype, log_name, offset=False))
            loop.run_until_complete(future)

        ### 'tenders/assigned_bidders
        scheduleitemproconid_list_sql = f"""
            SELECT scheduleItemProConId 
            FROM clean_current_procon_tenders_schedule_items 
            WHERE extract_type = '{extract_type}' 
            AND typeTitle = 'Shortlist' 
            """
        out, scheduleitemproconid_list = hc.query_exec(scheduleitemproconid_list_sql, __name__, to_list=True)
        #print(scheduleitemproconid_list)
        datatype = 'tenders/assigned_bidders'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', scheduleitemproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        ### 'tenders/submissions
        scheduleitemproconid_list_sql = f"""
            SELECT scheduleItemProConId 
            FROM clean_current_procon_tenders_schedule_items 
            WHERE extract_type = '{extract_type}' 
            AND typeTitle = 'Despatch/Tracking' """
        
        out, scheduleitemproconid_list = hc.query_exec(scheduleitemproconid_list_sql, __name__, to_list=True)
        #print(scheduleitemproconid_list)
        datatype = 'tenders/submissions'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', scheduleitemproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)


        ###'tenders/bidder_contacts'
        bidderproconid_list_sql = f"""
            SELECT bidderProConId 
            FROM clean_current_procon_tenders_bidders 
            WHERE extract_type = '{extract_type}' """
        
        out, bidderproconid_list = hc.query_exec(bidderproconid_list_sql, __name__, to_list=True)
        #print(bidderproconid_list)
        datatype = 'tenders/bidder_contacts'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', bidderproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)


        ###'tenders/schedule_items_notes'
        scheduleitemproconid_list_sql = f"""
            SELECT scheduleitemproconid 
            FROM clean_current_procon_tenders_schedule_items 
            WHERE extract_type = '{extract_type}' 
            ORDER BY to_number(scheduleitemproconid)
            """

        out, scheduleitemproconid_list = hc.query_exec(scheduleitemproconid_list_sql, __name__, to_list=True)
        #print(scheduleitemproconid_list)
        datatype = 'tenders/schedule_items_notes'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', scheduleitemproconid_list, datatype, log_name, offset=False))
            loop.run_until_complete(future)


        ###'tenders/schedule_items_notes_files'
        scheduleitemproconid_list_sql = f"""
            SELECT ATTACHMENTPROCONID 
            FROM CLEAN_CURRENT_PROCON_TENDERS_SCHEDULE_ITEM_NOTES 
            WHERE extract_type = '{extract_type}' 
            AND ATTACHMENTURL IS NOT NULL
            """

        out, scheduleitemproconid_list = hc.query_exec(scheduleitemproconid_list_sql, __name__, to_list=True)
        #print(scheduleitemproconid_list)
        datatype = 'tenders/schedule_items_notes_files'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for api in get_api.API:
            base_url =  f"{hc.apiurl}{api}"
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, '', scheduleitemproconid_list, datatype, log_name, offset=False))
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

