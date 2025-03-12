"""
API:
1) get the current user's buyerProConId from user details
    https://woodside.crm.connect.aveva.com/proconRestApi/procon/v1/executing-user/user-details
2) assign the Contract Administrator role to current user
    https://woodside.crm.connect.aveva.com/proconRestApi/procon/v1/post-awards/contract-role-assignments?contractProConReference={contract procon ref}
"""
import requests 
import datetime as dt
import urllib.parse
import api_common as hc
import logging
import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor
warnings.filterwarnings('ignore')

# SET THIS ACTION PARAMETER (True: assign contract admin role ; False: remove contract admin role)

ADD_ROLE_CONTRACT_ADMIN =  True

USE_OTHER_USER = False
OTHER_USER_buyerProConId = 2371
OTHER_USER_assignedToName = "Geoff Kellie"


print('Start of loading process')
header = hc.header
starttime = hc.starttime
dtstamp = starttime.strftime("%Y%m%d%H%M%S")
logging.basicConfig(
    filename=f'procon-{hc.legacy_org.lower()}-data-access-{dtstamp}.log', 
    encoding='utf-8', 
    format='%(asctime)s : %(message)s', 
    level=logging.INFO
    )
logger = logging.getLogger(__name__)

col_PROCONID = 0
col_CONTRACTPROCONREFERENCE = 1

def change_contract_admin_role(session, assigned_to_proconid, proconid, proconref, action):
    url_string= (
        f"https://{hc.apihost}/proconRestApi/procon/v1/post-awards/"
        f"contract-role-assignments?contractProConReference={urllib.parse.quote(proconref)}"
        ) 
    
    data = {"contractRoleType": "ContractAdministrator",
        "assignedToProConId": int(assigned_to_proconid),
        "assignedToType": "User",
        "assignedToName": hc.procon_user,
        "contractRoleName": "Contract Administrator",
        "isValid": "true",
        "parentProConId": int(proconid),
        "parentProConType": "PostAward",
        "isMandatory": "false",
        "isInherited": "false"}


    '''
    sql_check_admin_access = f"""
        SELECT 1 AS ADMIN_ACCESS 
        FROM CLEAN_CURRENT_PROCON_POST_AWARDS_CONTRACT_ROLE_ASSIGNMENTS
        WHERE CONTRACTROLETYPE  = 'ContractAdministrator'
        AND ASSIGNEDTOPROCONID = %s
        AND PARENTPROCONID = %s 
        AND EXTRACT_TYPE = '{hc.extract_type}'
    """
    parameters = (assigned_to_proconid, proconid)
    out, admin_access_check = hc.query_exec(sql_check_admin_access, __name__, to_list=True, params=parameters)
    count_admin_access = len(admin_access_check)
    '''
    #count_admin_access = 0              # TODO to be removed
    url_parse = urllib.parse.quote(url_string)
    status = None
    try:
        if action == "ADD": 
            #if count_admin_access<=0:   # TODO to be removed

            log_name = "Add contract admin role"          
            response=session.post(url_string,headers=header,verify=False,data=data)
            status = response.status_code 
            if status not in (200,201):
                print(f"Unable to add role to {proconid} Status code:{status}") 
                hc.insert_procon_log(log_name, url_parse,status, response.text, proconid, 'FAILED')   
                print(url_string)
            #else:
            #    print(f"Unable to add role. {assigned_to_proconid} already has contract admin role for {proconref}.")


        elif action == "REMOVE":
            #if count_admin_access>0:
            log_name = "Remove contract admin role"            
            response=requests.delete(url_string,headers=header,verify=False,data=data)
            #else:
            #    print(f"Unable to remove role. {assigned_to_proconid} does not has contract admin role for {proconref}.")

        if response.status_code == 201:
            hc.insert_procon_log(log_name, url_parse,response.status_code, '', proconid)
        else:
            hc.insert_procon_log(log_name, url_parse,response.status_code, response.text, proconid, 'FAILED')

    except Exception as err:
        print("Something went wrong when tried to assign admin role: " + str(err)) 
    
    return status


def request(session,assigned_proconid,  proconid, contractproconref, log_name ):
    status = change_contract_admin_role(session, assigned_proconid, proconid, contractproconref, "ADD")
    log_text = f"{proconid} | {contractproconref} | {status}"
    print(".", end="", flush=True)          
    logger.info(log_text)


async def push_api(contracts, assigned_proconid, logname):
    index_list = range(len(contracts))
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            for i in index_list:
                proconid = contracts[i][col_PROCONID]
                contractproconref = contracts[i][col_CONTRACTPROCONREFERENCE]
                loop.run_in_executor(executor, 
                                     request, 
                                     *(session,
                                       assigned_proconid, 
                                       proconid, 
                                       contractproconref, 
                                       logname) 
                                       )
                


###### MAIN ######

is_err = False

try:       
    url_string = f"https://{hc.apihost}/proconRestApi/procon/v1/executing-user/user-details"
    url_parse = urllib.parse.quote(url_string)
    log_name = "Add access to open contract: get user details"

    if USE_OTHER_USER:
        assigned_to_proconid = OTHER_USER_buyerProConId
        assigned_to_name = OTHER_USER_assignedToName
        hc.insert_procon_log(log_name, '', '', assigned_to_proconid)
    else:
        response=requests.get(url_string,headers=header,verify=False)
        if response.status_code == 200:
            response_data = response.json()
            assigned_to_proconid = response_data["buyerProConId"]
            hc.insert_procon_log(log_name, url_parse,response.status_code, '', assigned_to_proconid)
        else:
            is_err = True
            print(f"\n{str(response.text)}")
            hc.insert_procon_log(log_name, url_parse,response.status_code, response.text, '', 'FAILED')
    
    # assigning Contract Admin role if user don't have it
    if ADD_ROLE_CONTRACT_ADMIN and not is_err:
        print(f"Assigning Contract Admin role to {assigned_to_proconid} | {hc.previous_extract_type}")
        #log_name = 'Add access to open contract: add admin access'
        sql_from_proconid = f"""
        SELECT * FROM (
            SELECT PROCONID, CONTRACTPROCONREFERENCE 
            FROM clean_current_procon_Project_depts_Post_Awards   
            WHERE extract_type = '{hc.extract_type}' 
            --AND state = 'Open' 
            UNION 
            SELECT PROCONID, CONTRACTPROCONREFERENCE 
            FROM clean_current_procon_organisation_Post_Awards
            WHERE extract_type = '{hc.extract_type}' 
            --AND state = 'Open' 

        ) WHERE PROCONID NOT IN (
            SELECT PROCONID 
            FROM CLEAN_CURRENT_PROCON_POST_AWARDS_CONTRACT_ROLE_ASSIGNMENTS
            WHERE extract_type = '{hc.previous_extract_type}'
            AND ASSIGNEDTOPROCONID = '{assigned_to_proconid}'
            AND CONTRACTROLETYPE  = 'ContractAdministrator'
            )
            ORDER BY to_number(PROCONID)
        """
        results = hc.cur.execute(sql_from_proconid).fetchall()
        
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(push_api(results, assigned_to_proconid, log_name))
        loop.run_until_complete(future) 




    # removing Contract Admin role
    if not ADD_ROLE_CONTRACT_ADMIN and not is_err:
        print(f"Removing Contract Admin role to {assigned_to_proconid} ")        
        #log_name = 'Removing contract admin role'
        remove_role_from_proconid = f"""
            WITH post_award AS (
                SELECT PROCONID, CONTRACTPROCONREFERENCE, EXTRACT_TYPE 
                FROM clean_current_procon_Project_depts_Post_Awards   
                UNION 
                SELECT PROCONID, CONTRACTPROCONREFERENCE, EXTRACT_TYPE 
                FROM clean_current_procon_organisation_Post_Awards
            )
            SELECT DISTINCT p.PROCONID, p.CONTRACTPROCONREFERENCE
            FROM post_award p
            WHERE EXTRACT_TYPE = '{hc.extract_type}'
            ORDER BY to_number(p.PROCONID)
        """ 

        results = hc.cur.execute(remove_role_from_proconid).fetchall()
        length=(len(results))          

        for i in range(length):           
            proconid = results[i][col_PROCONID]        
            contractproconref = results[i][col_CONTRACTPROCONREFERENCE]  
            change_contract_admin_role(assigned_to_proconid, proconid, contractproconref, "REMOVE")
            log_text = f"Contract: {proconid} | {contractproconref}"
            print(log_text)          
            logger.info(log_text)


    ####completion of assigning / removing role admin   

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





