

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
            
            last_mysql_id = r.get('oppo_kash_sigupservice_common_login_last_mysql_id')
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select  id,common_user_id,date_created ,android_id,device_details,device_token,email,ins_email,mobile,password from signupservice.common_login where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select  id,common_user_id,date_created ,android_id,device_details,device_token,email,ins_email,mobile,password from signupservice.common_login  order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppo_kash_sigupservice_common_login_last_mysql_id', last_record[0])

                user_id = None
                android_id_req = None
                device_details_req = None
                device_token_req = None
                email_req = None
                ins_email_req = None
                mobile_req = None
                password_req = None
                
                for record in records:
                    time.sleep(1)
                    if record[3] is not None:
                        android_id_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[3]},verify=False)
                    if record[4] is not None:
                        device_details_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[4]},verify=False)
                    if record[5] is not None:
                        device_token_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[5]},verify=False)
                    if record[6] is not None:
                        email_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[6]},verify=False)
                    if record[7] is not None:
                        ins_email_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[7]},verify=False)
                  
                    if record[8] is not None:
                        mobile_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[8]},verify=False)
                    if record[9] is not None:
                        password_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[9]},verify=False)
                    
                    android_id_response = ""
                    device_details_response = ""
                    device_token_response = ""
                    email_response = ""
                    ins_email_response = ""
                    mobile_response = ""
                    password_response = ""

                    if android_id_req is not None :
                        if android_id_req.status_code != 200:
                             print(android_id_req)
                             raise Exception('api-failure', android_id_req.text)
                        else:
                            android_id_response = android_id_req.text
                    if device_details_req is not None :
                        if device_details_req.status_code != 200:
                             print(device_details_req)
                             raise Exception('api-failure', device_details_req.text)
                        else:
                            device_details_response = device_details_req.text
                    if device_token_req is not None :
                        if device_token_req.status_code != 200:
                             print(device_token_req)
                             raise Exception('api-failure', device_token_req.text)
                        else:
                            device_token_response = device_token_req.text

                    if email_req is not None :
                        if email_req.status_code != 200:
                             print(email_req)
                             raise Exception('api-failure', email_req.text)
                        else:
                            email_response = email_req.text

                    if ins_email_req is not None :
                        if ins_email_req.status_code != 200:
                             print(ins_email_req)
                             raise Exception('api-failure', ins_email_req.text)
                        else:
                            ins_email_response = ins_email_req.text

                    if mobile_req is not None :
                        if mobile_req.status_code != 200:
                             print(mobile_req)
                             raise Exception('api-failure', mobile_req.text)
                        else:
                            mobile_response = mobile_req.text

                    if password_req is not None :
                        if password_req.status_code != 200:
                             print(password_req)
                             raise Exception('api-failure', password_req.text)
                        else:
                            password_response = password_req.text

                    doc = {
                                "android_id":android_id_response,
                                "device_details":device_details_response,
                                "device_token":device_token_response,
                                "email":email_response,
                                "ins_email":ins_email_response,
                                "mobile":mobile_response,
                                "password":password_response
                             }
                        
                    doc['id'] = 0 if record[0] is None  else record[0] 
                    doc['common_user_id'] = 0 if record[1] is None else record[1] 
                    doc['date_created'] = "" if record[2] is None else record[2] 
                    db.decrypted_oppo_kash_signup_service_common_login.insert_one(doc)

        except Error as e:
            print(e)
        
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   



