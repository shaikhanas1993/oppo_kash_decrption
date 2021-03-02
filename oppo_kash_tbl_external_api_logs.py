

#script to decrypt 
import time
from datetime import datetime,date
import mysql.connector
from mysql.connector import Error
import warnings
import redis

warnings.filterwarnings('ignore', message='Unverified HTTPS request')
import requests
import json
from requests.exceptions import Timeout
from requests.auth import HTTPBasicAuth
import pymongo
import urllib.parse
username = urllib.parse.quote_plus('datas')
password=urllib.parse.quote_plus('Data@321')
uri = "mongodb://%s:%s@10.100.50.132:27017/admin" % (
    username, password)
client = pymongo.MongoClient(uri)
db = client.oppoAnalytics

headers = {'Content-type': 'application/json','oppokey':"b3Bwb2thc2hlYWRlcjpwYXNzd29yZG9wcG9rYXNo"}
url = 'https://analyticsapi.realmepaysa.com/cms/users/decryptDataoppo'
try:

    connection = mysql.connector.connect(host='10.100.20.55',
                                        port='23306',
                                        user='opkash_mis',
                                        password='OP|<asH@m!s',
                                        connection_timeout= 86400)
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        try:

            r = redis.Redis(host='10.100.50.133', port=6379, db=0)
            print("connected  to redis")
            
            last_mysql_id = r.get('oppo_kash_tbl_external_api_logs_last_mysql_id')
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select id,date_created,mobile_number,request,response from loggingservice.tbl_external_api_logs where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select id,date_created,mobile_number,request,response from loggingservice.tbl_external_api_logs  order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppo_kash_tbl_external_api_logs_last_mysql_id', last_record[0])

              
                mobile_number_req = None
                request_req = None
                response_req = None
                
                
                
                for record in records:
                    time.sleep(1)
                    if record[2] is not None:
                        mobile_number_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[2]},verify=False)
                    if record[3] is not None:
                        request_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[3]},verify=False)
                    if record[4] is not None:
                        response_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[4]},verify=False)
                    

                    
                    
                    mobile_number_response = ""
                    request_response = ""
                    response_response = ""
                  
                  
                    

                    if mobile_number_req is not None :
                        if mobile_number_req.status_code != 200:
                             print(mobile_number_req)
                             raise Exception('api-failure', mobile_number_req.text)
                        else:
                            mobile_number_response = mobile_number_req.text


                    if request_req is not None :
                        if request_req.status_code != 200:
                             print(request_req)
                             raise Exception('api-failure', request_req.text)
                        else:
                            request_response = request_req.text


                    if response_req is not None :
                        if response_req.status_code != 200:
                             print(response_req)
                             raise Exception('api-failure', response_req.text)
                        else:
                            response_response = response_req.text

                    
                    

                    doc = {
                                "mobile_number":mobile_number_response,
                                "request":request_response,
                                "response":response_response,
                          }
                        
                    doc['ext_table_id'] = 0 if record[0] is None  else record[0] 
                    doc['date_created'] = "" if record[1] is None else record[1] 
                    db.decrypted_oppo_kash_tbl_external_log.insert_one(doc)

        except Error as e:
            print(e)
        
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   








