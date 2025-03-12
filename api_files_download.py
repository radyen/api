import gc
import requests
import pandas as pd
import datetime as dt
import argparse as ap
import os
import time
import errno
import logging
import asyncio
import urllib3
import warnings
from concurrent.futures import ThreadPoolExecutor


# common
#hwel
apihost = "perapp855.wde.woodside.com.au"
mytoken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlI0aGp0OGQzVEF4V2ZfX2k3YlREQ0E1bDllWSJ9.eyJpc3MiOiJwcm9jb246YXV0aC1jb21wYW55IiwiYXVkIjoicHJvY29uOmludGVncmF0aW9uIiwibmJmIjoxNzM2MTQ0MjUyLCJleHAiOjE3Mzg3MzYyNTIsImNsaWVudF9pZCI6ImludGVncmF0aW9uY2xpZW50Iiwic2NvcGUiOiJmdWxsLWFjY2VzcyIsInN1YiI6Ilc3NzE3NiIsInByb2NvbjpvaWQiOiIxIiwicHJvY29uOmlwIjoicHJvY29uOlByb0NvblNUUyIsInByb2Nvbjp1dCI6IjEifQ.LGA_g7LExHtjmPDgVwWy6lGkeyAZdjDDmbmA2JmTkXU3ebIlgn2CzeYftA_Sg1YUS8qbR3Jq1Ha8DZ7fbhUCWa9idVpTCIOmCu_AQhi_59c_OEhOroGN7ydIcDvrZTP2S5byXV3qyAmIKa_d_5Z06_zne2Sez98wdK2V_QCitX0kcz_cD_Ms4m4kqwjexkvu7HRKozgjVsy744aHy5s6CWBoXhHQpKxfzunx6Zq91aWybjkZzVCaZbvHIgV6Sam35hj9S2ObSCy4fVTgf2zQYCuXlqzjvk713lu_d0Z4r7O94vF3oqprI7WY-BN5py8efUZXcIKWBlDOlHRI49QF3w"
#directory= r"./procon-hwel"
directory="C:\\Temp\\AWS\\hwel"

#hbhp
#apihost = "woodside.crm.connect.aveva.com"
#mytoken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkFhVTFXTl84UkpjOXYyOFRRSWRNU3JQMl9zTSJ9.eyJpc3MiOiJwcm9jb246YXV0aC1jb21wYW55IiwiYXVkIjoicHJvY29uOmludGVncmF0aW9uIiwibmJmIjoxNzM3MzMwMDI4LCJleHAiOjE3Mzk5MjIwMjgsImNsaWVudF9pZCI6ImludGVncmF0aW9uY2xpZW50Iiwic2NvcGUiOiJmdWxsLWFjY2VzcyIsInN1YiI6InJhZHkuZW5kcmFzbW9yb0B3b29kc2lkZS5jb20iLCJwcm9jb246b2lkIjoiMSIsInByb2NvbjppcCI6Ildvb2RzaWRlIiwicHJvY29uOnV0IjoiMSJ9.pGjTn4EBqNdSuAm7Ho3ypbVinMuq-h224bA2Ll4MO1UJXxyrz1I6y3WKVCRNOA0RwcHL80g5ul-H-U90h8hsjtBDK_z7HowmXFzVV9W5Njva4NRUtAem2psWxzcYS8oW0XjTvGDQd2JipQDdMIlcqAML71cQrQVTV8DGCUUulEPijBLL39i8MrD2l5qzgn6i_d1D4RNcaSXKiVRgYd35enj-HTQl3dLhaI1yfn02IJO8hV4Wiv8aJK354HC3C3-8tNBIBuhz_ARFd9hKJMKaWOrwJZYWmD-JYKwXr2y5Y7_Q3M5WXulrerRzWOXn33JTfIPbVqHQVTe7EkSgQ4cpqg"
#directory= "C:\\Temp\\AWS\\hbhp"



os.environ['NO_PROXY'] = apihost
header = {'Authorization': "Bearer " + mytoken}
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

warnings.filterwarnings('ignore')

print('Start of loading process')

starttime = dt.datetime.now()
dtstamp = starttime.strftime("%Y%m%d%H%M%S")

logging.basicConfig(
    filename=f'logs/procon-files-{dtstamp}.log',
    encoding='utf-8',
    format='%(message)s',
    level=logging.INFO
    )
logger = logging.getLogger(__name__)
logger.info('Start of loading process')

#df_out = pd.DataFrame(columns=['SEQ', 'UID', 'STATUS', 'API', 'FILEPATH'])

token_gen_time_min=30


def df_add_row(val1, val2, val3, val4, val5):
    logger.info(f"{val1}|{val2}|{val3}|{val4}|{val5}")
    #print(f"{val3} | {val2}")
    print(f"{val1}|{val3} ", end="", flush=True)
    #df_out.loc[-1] = [val1, val2, val3, val4, val5]
    #df_out.index = df_out.index + 1
    #df_out.sort_index(inplace=True)
    #return df_out

def request(session, url_api, fileproconid, filefullname, contractproconref, target_folder, org, uid, seq ):
    error_text = ""
    url_string = f"{url_api}{fileproconid}/contents"
    print(url_string)
    with session.get(url_string, headers=header, verify=False) as response:
        response_status = response.status_code
        print(f"STATUS: {response_status}")
        abs_path = os.path.join(directory, str(contractproconref), target_folder, filefullname)

        try: 

            if str(response_status)=="200":
                print(f"RESPONSE: {str(response_status)}")

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

        except Exception as e:
            error_text = str(e)
            print(error_text)

        df_add_row(seq, uid, response_status, url_string, abs_path)




async def get_api_response(base_url, df_files):
    with ThreadPoolExecutor(max_workers=1) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            df = df_files.reset_index()

            for index, file in df.iterrows():
                seq = file ['SEQ']
                contractproconref = file['CONTRACTPROCONREFERENCE']
                fileproconid = file['FILEPROCONID']
                filefullname = file['API_FILENAME']
                doctype = file['DOCUMENT_TYPE']
                org = file['LEGACY_ORG']
                uid = file['UID']

                loop.run_in_executor(executor,
                                     request,
                                     *(session,
                                       base_url,
                                       fileproconid,
                                       filefullname,
                                       contractproconref,
                                       doctype,
                                       org,
                                       uid,
                                       seq
                                       )
                                    )



parser = ap.ArgumentParser()
parser .add_argument("-f", "--SourceFile", help="csv file name")
parser .add_argument("-o", "--LegacyOrg", default="", help="Legacy Organisation eg. HWEL or HBHP")
parser .add_argument("-t", "--DocType", default = "", help="Refer to document types")
parser .add_argument("-s", "--SeqId", default = 0)

args = parser.parse_args()
source_file = args.SourceFile
org = args.LegacyOrg
doctype = args.DocType
seqid = int(args.SeqId)

print(args)
df = None

try:
    #df = pd.read_excel(source_file, sheet_name=org)
    for df in  pd.read_csv(source_file, chunksize=100):
        if doctype != "":
            df = df.query(f"DOCUMENT_TYPE == '{doctype}'")

        df = df.query(f"SEQ > {seqid}")
        # df = df.query("CONTRACTPROCONREFERENCE == '4600002714-01'")
        if len(df)>0:

            #print(df.head())

            session_time = dt.datetime.now()
            time_change = dt.timedelta(minutes=token_gen_time_min)
            time.sleep(2)
            # only download files that have not beed downloaded in previous extract
            base_url = f"https://{apihost}/proconRestApi/procon/v1/files/"

            if doctype == "06 Tender Submissions":
                base_url = f"https://{apihost}/proconRestApi/procon/v1/submission-files/"

            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_api_response(base_url, df))
            loop.run_until_complete(future)


except Exception as e:
    #df_out.to_csv(f'logs/procon-files-temp-{dtstamp}.csv', index=False)
    print(e)

finally:
    #df_out.to_csv(f'logs/procon-files-{dtstamp}.csv', index=False)
    endtime = dt.datetime.now()
    log_start = f'Started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}'
    log_finish = f'Finished at: {endtime.strftime("%Y-%m-%d %H:%M:%S")}'
    log_total = f'Total time {endtime-starttime}'
    log_elapse_time = f'\n{log_start}\n{log_finish}\n{log_total}'
    print(f'\n{log_elapse_time}')
    logger.info(f'{log_elapse_time}\n\nEnd of loading process')


