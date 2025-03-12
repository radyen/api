import requests dtcommunication-state-transitions/files
import pandas as pd
import datetime as dt
import urllib.parse
import api_common as hc
import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')
starttime = dt.datetime.now()
dtstamp = starttime.strftime("%Y%m%d%H%M%S")

logger = hc.setup_log(f"procon-{hc.legacy_org.lower()}-load-{dtstamp}.log")

header = hc.header


def load_procon_extract_from_prev(data_type, proconid): 
    sql_ins_prev_comm = f"""
    INSERT INTO PROCON_EXTRACT (DATA, DATA_TYPE, API_URL, PARENT_LINKAGE_ID, DATA_FORMAT, INGEST_TIMESTAMP, EXTRACT_TYPE)
      SELECT DATA, TRIM(DATA_TYPE), API_URL, PARENT_LINKAGE_ID, DATA_FORMAT, INGEST_TIMESTAMP, '{hc.extract_type}' 
      FROM PROCON_EXTRACT 
      WHERE EXTRACT_TYPE = '{hc.previous_extract_type}' 
      AND PARENT_LINKAGE_ID IN (
        SELECT communicationProConId 
        FROM clean_current_procon_post_awards_communications 
        WHERE EXTRACT_TYPE = '{hc.extract_type}' 
        AND  PROCONID = {proconid}
      ) 
      AND TRIM(DATA_TYPE) ='{data_type}'
      AND DATA <> '[]'
    """
    #print(sql_ins_prev_comm)
    hc.query_exec(sql_ins_prev_comm, __name__)


async def get_api_response(base_url, type, id_list, datatype, logname, offset=True):
    #print(f"{datatype}")
    new_api_list = [
        'post_awards/business_process_roles', 'post_awards/risk_covers','generic_reviews/contributions','generic_reviews/contribution_summary',
        'generic_reviews/decision_sets','generic_reviews/internal_notes','generic_reviews/reviewers','projectdepts/business_process_roles'
        ]
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            for id in id_list:
                api_url = ""
                str_id = str(id)
                if datatype.strip() in new_api_list:
                    api_url = base_url.replace("{contractproconreference}",str_id).replace("{genericreviewproconid}",str_id).replace("{businessprocessroleproconid}",str_id)  
                    if offset:
                        api_url = api_url + "offset="
                else:
                    if offset:
                            api_url = f"{base_url}{id}/{type}?offset="
                            if (len(type)==0 and len(id) == 0):
                                api_url = f"{base_url}?offset="
                            if datatype == "communication-state-transitions/":
                                datatype = f"{datatype}{type}"
                            
                    else:
                        api_url = f"{base_url}{id}/{type}"
                        if datatype.strip() == 'post-awards/tender-library-items':
                            api_url = f"{base_url}{type}?contractProConReference={urllib.parse.quote(str_id)}"
                loop.run_in_executor(executor, hc.request, *(session, id, api_url, datatype, logname, offset) )





if __name__ == '__main__':
    print('Start of loading process')
    log_name = 'Contract snowflake load'
    api_sheet_name = 'API-CONTRACTS'
    
    #get api list
    apilist = pd.read_excel(hc.api_info,sheet_name=api_sheet_name)
    apilistnooffset=apilist[ (apilist.ACTIVE == 'YES') & (apilist.TYPE == 'NO_OFFSET')] 
    apilistyesoffset=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'OFFSET')] 
    apilistnooffsetwithprojetlist=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'NO_OFFSET_WITH_PROJECTLIST')] 
    apilistlookup=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'LOOKUP')] 
    apilistpostawards=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'POST_AWARDS')] 
    apilistcommunications=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'COMMUNICATIONS')] 
    apilistcommstatetrans=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'COMM_STATE_TRANS')] 
    apilistcompanyuser=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'COMPANY_USER')] 
    apilistnotification=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == 'NOTIFICATION')] 

    logger.info(f"API LIST:\n'{apilist}")
    no_list = ['']

    try:
        # #### load nooffset ############################ 
        for datatype in apilistnooffset.API:
            full_url =  f"{hc.apiurl}{datatype}"
            print(f"\nAPI: {full_url}")
            hc.get_response(full_url, datatype, log_name)

        # #### load offset ##############################
        for datatype in apilistyesoffset.API:
            full_url =  f"{hc.apiurl}{datatype}"
            print(f"\nAPI: {full_url}")
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', no_list, datatype, log_name))
            loop.run_until_complete(future) 

        # #### load no offset with project loop #########   
        projectdept_l3level_no_offset = ['control-accounts','post-awards/financial-forecasts','conversion-rates','post-awards/conversion-rates','post-awards']                   
        sql_sel_tier3proconid = f"""
             SELECT DISTINCT tier3ProConId  
             FROM clean_current_procon_organisation_project_depts 
             WHERE extract_type = '{hc.extract_type}'
            """
        out, project_list = hc.query_exec(sql_sel_tier3proconid, __name__, to_list=True)

        for datatype in apilistnooffsetwithprojetlist.API:
            for pd_level_no_offset in projectdept_l3level_no_offset:
                datatype_lvl = f"{datatype}{pd_level_no_offset}"
                full_url =  f"{hc.apiurl}{datatype}"
                print(f"\nAPI: {full_url}<id>/{pd_level_no_offset}")   
                if pd_level_no_offset == "post-awards":
                    hc.get_project_response_offset(full_url, pd_level_no_offset, project_list, datatype_lvl, log_name)
                else:
                    hc.get_project_response(full_url, pd_level_no_offset, project_list, datatype_lvl, log_name)


        # #### load lookup ############################## 
        lookup_l3level = [
            'addresses','alerts','communication-template-reporting-categories','countries','currencies','event-formulas','internal-codes','legal-forms',
            'manual-grades','mapping-templates','message-types','organisation-categories','project-dept-categories','risk-types','timezones',
            'trade-sectors','units-of-measure','validation-rules','widget-categories'
            ]
        
        for datatype in apilistlookup.API:
            for lookup_level in lookup_l3level:
                datatype_lvl = f"{datatype}{lookup_level}"
                full_url =  f"{hc.apiurl}{datatype}"
                print(f"API: {full_url}/<id>/{lookup_level}")   
                result = hc.get_lookup_response(full_url, lookup_level, project_list, datatype_lvl, log_name)


        # #### load company user ########################
        for datatype in apilistcompanyuser.API:
            result = 0
            full_url =  f"{hc.apiurl}{datatype}"
            print(f"API: {full_url}") 
            result = hc.get_users_response_offset(full_url, datatype.replace("/",''), log_name)

        # #### load notifications ########################
        sel_list = ['0']
        pa_level = ''    
        for datatype in apilistnotification.API:
            full_url =  f"{hc.apiurl}{datatype}"
            print(f"API: {full_url}") 
            result = hc.get_users_response_offset(full_url, datatype.replace("/",''), log_name)

        proconid_loop = False
        if (len(apilistpostawards.API) > 0 or len(apilistcommunications.API)>0):
            proconid_loop = True
            print("\nAPI: Post-awards and Communications")

        # ############  Start of poconid loop for post-award and communications ############################
        if proconid_loop:                
            ##get all available proconid
            insert_st = f"""
                INSERT INTO PROCON_LOAD_PROCONID (proconid, data_loaded, is_active, extract_type)   
                ( SELECT DISTINCT proconid , 'N' data_loaded, 'Y' is_active, extract_type 
                    FROM clean_current_procon_project_depts_post_awards 
                    WHERE extract_type = '{hc.extract_type}' 
                    /* AND state = 'Open' */  
                    UNION 
                    SELECT DISTINCT proconid , 'N' data_loaded,'Y' is_active, extract_type 
                    FROM clean_current_procon_organisation_post_awards 
                    WHERE extract_type = '{hc.extract_type}' 
                    /* AND state = 'Open' */
                    ) 
                    MINUS 
                    SELECT DISTINCT proconid , 'N' data_loaded, 'Y' is_active, extract_type 
                    FROM PROCON_LOAD_PROCONID WHERE extract_type = '{hc.extract_type}'  
                """
            
            hc.query_exec(insert_st, __name__)
            #print(insert_st)

            sql_sel_proconid = (
                "SELECT PROCONID " 
                "FROM PROCON_LOAD_PROCONID " 
                f"WHERE EXTRACT_TYPE = '{hc.extract_type}' " 
                "AND  DATA_LOADED = 'N' " 
                "AND IS_ACTIVE = 'Y' " 
                "AND PROCONID NOT IN ( " 
                "    SELECT DISTINCT info4 FROM procon_log " 
                f"    WHERE EXTRACT_TYPE = '{hc.extract_type}' " 
                "    AND name like 'Add access to%' " 
                "    AND info3  IN ( " 
                "    '{\"message\":\"Reopen Contract - you cannot perform this action. You may not be authorised.\",\"statusCode\":403,\"details\":[\"Reopen Contract - you cannot perform this action. You may not be authorised.\"]}', " 
                "    '{\"message\":\"You cannot close this contract as it currently has active alerts.\",\"statusCode\":404,\"errorNumber\":60140,\"details\":[\"You cannot close this contract as it currently has active alerts.\"]}' " 
                "    ) ) ORDER BY to_number(proconid) "
            )
            
            out, get_act_proconid = hc.query_exec(sql_sel_proconid, __name__, to_list=True)
            #print(sql_sel_proconid)

            total_act_proconid = len(get_act_proconid)

            #print(f"PROCONIDs: {get_act_proconid}")
            for act_proconid in range(total_act_proconid):
                proconid = get_act_proconid[act_proconid]

                #print(f"\nStart loading of {proconid}")

                update_st = (
                    "UPDATE PROCON_LOAD_PROCONID "
                    "SET DATA_LOADED = 'X',LOAD_TIME = SYSDATE() "
                    f"WHERE EXTRACT_TYPE = '{hc.extract_type}' "
                    f"AND  PROCONID =  {(proconid)}"
                    )
                hc.cur.execute(update_st)        

                # #### load post-award ##############################
                for datatype in apilistpostawards.API:  
                    #print(f"API: {id}")
                    ##Load postawards
                    sql_ctproconref_from = (
                        "FROM ( SELECT CONTRACTPROCONREFERENCE,PROCONID " 
                        "       FROM clean_current_procon_organisation_post_awards "
                        f"       WHERE  EXTRACT_TYPE = '{hc.extract_type}' "
                        "       UNION "
                        "       SELECT CONTRACTPROCONREFERENCE, PROCONID "
                        "       FROM clean_current_procon_Project_depts_Post_Awards "
                        f"       WHERE EXTRACT_TYPE = '{hc.extract_type}' "
                        f"   ) WHERE PROCONID = {proconid}"
                    )

                    sql_sel_proconid = f"SELECT PROCONID {sql_ctproconref_from}" 
                    out, contract_list = hc.query_exec(sql_sel_proconid, __name__, to_list=True)
                    
                    #print ('for postaward contract_list:'+str(contract_list))

                    sql_sel_proconref = f"SELECT CONTRACTPROCONREFERENCE {sql_ctproconref_from}" 
                    out, contract_procon_ref_list = hc.query_exec(sql_sel_proconref, __name__, to_list=True)
                    
                    
                    postaward_l3level_no_offset = [
                        'contract-commitment-snapshot', 'financial-health', 'financial-health/call-offs?reportingCategory=0', 'currency-rates',
                        'call-off-counts', 'call-off-restrictions', 'tender-library-items', 'call-offs-conversion-rates', 'conversion-rates',
                        'line-items-summary', 'product-codes'
                        ]
                    
                    #################
                    #postaward_l3level_no_offset = []
                    #################

                    for pa_level in postaward_l3level_no_offset:
                        datatype_lvl = f"{datatype}{pa_level}"
                        full_url =  f"{hc.apiurl}{datatype}"
                        result = 0
                        sel_list = contract_list
                        if pa_level == 'tender-library-items':
                            sel_list = contract_procon_ref_list

                        loop = asyncio.get_event_loop()
                        future = asyncio.ensure_future(get_api_response(full_url, pa_level, sel_list, datatype_lvl, log_name, offset=False))
                        loop.run_until_complete(future)

                    # OUT: 'claimable-progress-measures', 'claimable-progress-communications', 'progress-measures', 'progress-measure-groups',
                     
                    postaward_l3level_offset = [
                        'contract-role-assignments', 'obligations', 'communications', 'line-items', 'rates',  
                        'financial-breakdown', 'contract-breakdown', 'claimable-items', 'breakdown-items'
                        ]
                    
                    #################
                    #postaward_l3level_offset = []
                    #################
                                        
                    for pa_level in postaward_l3level_offset:
                        result = 0
                        datatype_lvl = f"{datatype}{pa_level}"
                        full_url =  f"{hc.apiurl}{datatype}"
                        loop = asyncio.get_event_loop()
                        future = asyncio.ensure_future(get_api_response(full_url, pa_level, contract_list, datatype_lvl, log_name))
                        loop.run_until_complete(future)


                # #### load communication ###########################
                for datatype in apilistcommunications.API:
                    #print(f"API: {id}")
                    '''
                    sql_sel_commproconid_cur = (
                        "SELECT communicationProConId "
                        "FROM clean_current_procon_post_awards_communications "
                        f"WHERE EXTRACT_TYPE = '{hc.extract_type}' "
                        f"AND  PROCONID = {proconid}"
                    )
                    out, comms_list_cur = hc.query_exec(sql_sel_commproconid_cur, __name__, to_list=True)
                    '''

                    sql_sel_commproconid_prev = (
                        "SELECT communicationProConId "
                        "FROM clean_current_procon_post_awards_communications "
                        f"WHERE EXTRACT_TYPE = '{hc.previous_extract_type}' "
                        f"AND  PROCONID = {proconid}"
                    )
                    out, comms_list_prev = hc.query_exec(sql_sel_commproconid_prev, __name__, to_list=True)
                    

                    sql_sel_commproconid_process = (
                        "SELECT communicationProConId "
                        "FROM clean_current_procon_post_awards_communications "
                        f"WHERE EXTRACT_TYPE = '{hc.extract_type}' "
                        f"AND  PROCONID = {proconid} "
                        f"AND  communicationProConId NOT IN ({sql_sel_commproconid_prev})" 
                    )
                    out, comm_id_list = hc.query_exec(sql_sel_commproconid_process, __name__, to_list=True)


                    #remove comm that has been loaded in previous extract from comms_list
                    #comm_id_list = list(set(comms_list_cur) - set(comms_list_prev))
                    #count_comm_current_extract = len(comms_list_cur)
                    count_comm_previous_extract = len(comms_list_prev)
                    count_comm_tobe_proccesed = len(comm_id_list)

                    log_text = (f"ProconId: {proconid}|"
                                #f"Total Comm contract: {count_comm_current_extract}|"
                                f"Comm exists from previous extract: {count_comm_previous_extract}|"
                                f"Comm to be extracted: {count_comm_tobe_proccesed}")
                    
                    # OUT: 'communication-progress-measures'
                    comm_l3level_offset = [
                        'attachments', 'expenditures', 'generic-reviews', 'communication-claimable-items', 
                        'commitment-changes', 'line-items','line-item-changes'
                        ]

                    # OUT: 'stories',           
                    comm_l3level_no_offset = [
                       'line-items-summary', 'communication-state-transitions', 'marked-replies'
                        ]

                    comm_all_types = comm_l3level_offset + comm_l3level_no_offset
                    # TO BE REMOVED ############# filter the comm types
                    #sel_comms_offset = [ item for item in comm_l3level_offset if item == 'attachments' ]
                    #sel_comms_no_offset = [ item for item in comm_l3level_no_offset if item not in ['communication-state-transitions', 'marked-replies' ] ]

                    for comm_type in comm_all_types:
                        result = 0
                        #datatype = f"{id.replace(hc.apiurl,'')}{comm_type}"
                        datatype_lvl = f"{datatype}{comm_type}"
                        full_url =  f"{hc.apiurl}{datatype}"

                        # bulk load if already existed in previous extract
                        if count_comm_previous_extract > 0:
                            load_procon_extract_from_prev(datatype_lvl, proconid)

                        # use async to speed up the process
                        loop = asyncio.get_event_loop()
                        if comm_type in comm_l3level_offset:
                            future = asyncio.ensure_future(get_api_response(full_url, comm_type, comm_id_list, datatype_lvl, log_name))
                        if comm_type in comm_l3level_no_offset: 
                            future = asyncio.ensure_future(get_api_response(full_url, comm_type, comm_id_list, datatype_lvl, log_name, offset=False))
                        loop.run_until_complete(future)

                update_st = (
                    "UPDATE PROCON_LOAD_PROCONID "  
                    "SET DATA_LOADED = 'Y',LOAD_TIME = SYSDATE() "
                    f"WHERE EXTRACT_TYPE = '{hc.extract_type}' "
                    f"AND PROCONID = {proconid} "
                )

                hc.query_exec(update_st, __name__)

        # ##### End of proconId loop ##### #


        # #### load Communication State Transitions ##################
        for datatype in apilistcommstatetrans.API:
            #print(f"API: {id}")
            sql_sel_commstatetransition = (
                 "SELECT DISTINCT communicationStateTransitionProConId "
                 "FROM clean_current_procon_communications_communication_state_transitions "
                f"WHERE extract_type = '{hc.extract_type}' "
                )
            
            out, commstatetransition_list = hc.query_exec(sql_sel_commstatetransition, __name__, to_list=True)

            result = 0
            #datatype = f"{id.replace(hc.apiurl,'')}files"
            #datatype = f"{datatype}"
            full_url =  f"{hc.apiurl}{datatype}"
            print(f"API: {full_url}")  

            comm_type = 'files'
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, comm_type, commstatetransition_list, datatype, log_name))
            loop.run_until_complete(future)            



        ## ADDITIONAL APIs (previously only for HWEL) ###################################
        log_name = 'Contract snowflake load - additional API'

        ### post_awards
        contractproconreference_list_sql = f"""
            SELECT DISTINCT contractproconreference 
            FROM clean_current_procon_organisation_post_awards 
            WHERE extract_type = '{hc.extract_type}' 
            UNION
            SELECT distinct contractproconreference 
            FROM clean_current_procon_Project_depts_Post_Awards 
            WHERE extract_type = '{hc.extract_type}'
            ORDER BY contractproconreference
            """
        out, contractproconreference_list = hc.query_exec(contractproconreference_list_sql, __name__, to_list=True)

        datatype = 'post_awards/business_process_roles'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', contractproconreference_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        ###'post_awards/risk_covers'
        datatype = 'post_awards/risk_covers'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")           
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', contractproconreference_list, datatype, log_name, offset=False))
            loop.run_until_complete(future)


        ### generic_reviews
        genericreviewproconid_list_sql = f"""
            SELECT DISTINCT genericreviewproconid 
            FROM clean_current_procon_communications_generic_reviews 
            WHERE extract_type = '{hc.extract_type}' 
            ORDER BY to_number(genericreviewproconid)
            """
        out, genericreviewproconid_list = hc.query_exec(genericreviewproconid_list_sql, __name__, to_list=True)

        datatype = 'generic_reviews/contributions'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")            
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', genericreviewproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        ###'generic_reviews/contribution_summary'
        datatype = 'generic_reviews/contribution_summary'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")            
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', genericreviewproconid_list, datatype, log_name, offset=False))
            loop.run_until_complete(future)

        ###'generic_reviews/decision_sets'
        datatype = 'generic_reviews/decision_sets'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")            
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', genericreviewproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        ###'generic_reviews/internal_notes'
        datatype = 'generic_reviews/internal_notes'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")            
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', genericreviewproconid_list, datatype, log_name, offset=False))
            loop.run_until_complete(future)

        ###'generic_reviews/reviewers'
        datatype = 'generic_reviews/reviewers'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")            
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', genericreviewproconid_list, datatype, log_name, offset=True))
            loop.run_until_complete(future)

        ###'projectdepts/business_process_roles'
        businessprocessroleproconid_list_sql = f"""
            SELECT DISTINCT businessprocessroleproconid 
            FROM clean_current_procon_post_awards_business_process_roles 
            WHERE extract_type = '{hc.extract_type}' 
            ORDER BY to_number(businessprocessroleproconid)
            """
        out, businessprocessroleproconid_list = hc.query_exec(businessprocessroleproconid_list_sql, __name__, to_list=True)

        datatype = 'projectdepts/business_process_roles'
        get_api=apilist[(apilist.ACTIVE== 'YES') & (apilist.TYPE == datatype)] 
        for apiurl in get_api.API:
            full_url =  f"{hc.apiurl}{apiurl}"
            print(f"\nAPI: {full_url}")            
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(full_url, '', businessprocessroleproconid_list, datatype, log_name, offset=True))
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

