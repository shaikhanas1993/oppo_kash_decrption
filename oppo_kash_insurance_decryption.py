

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
            
            last_mysql_id = r.get('oppo_kash_insurance_service_user_last_mysql_id')
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select id,user_id,date_created,assetimei1,assetimei2,customer_name,dob,email_id,final_user_plan,phone_no,policy_no from insuranceservice.user where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select id,user_id,date_created,assetimei1,assetimei2,customer_name,dob,email_id,final_user_plan,phone_no,policy_no from insuranceservice.user  order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppo_kash_insurance_service_user_last_mysql_id', last_record[0])

              
                assetimei1_req = None
                assetimei2_req = None
                customer_name_req = None
                dob_req = None
                email_id_req = None
                final_user_plan_req = None
                phone_no_req = None
                policy_no_req = None
                
                for record in records:
                    time.sleep(1)
                    if record[3] is not None:
                        assetimei1_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[3]},verify=False)
                    if record[4] is not None:
                        assetimei2_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[4]},verify=False)
                    if record[5] is not None:
                        customer_name_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[5]},verify=False)
                    if record[6] is not None:
                        dob_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[6]},verify=False)
                    if record[7] is not None:
                        email_id_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[7]},verify=False)

                    if record[8] is not None:
                        final_user_plan_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[8]},verify=False)
                  
                    if record[9] is not None:
                        phone_no_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[9]},verify=False)
                   
                    if record[10] is not None:
                        policy_no_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[10]},verify=False)
                    
                    assetimei1_response = ""
                    assetimei2_response = ""
                    customer_name_response = ""
                    dob_response = ""
                    email_id_response = ""
                    final_user_plan_response = ""
                    phone_no_response = ""
                    policy_no_response = ""
                    

                    if assetimei1_req is not None :
                        if assetimei1_req.status_code != 200:
                             print(assetimei1_req)
                             raise Exception('api-failure', assetimei1_req.text)
                        else:
                            assetimei1_response = assetimei1_req.text


                    if assetimei2_req is not None :
                        if assetimei2_req.status_code != 200:
                             print(assetimei2_req)
                             raise Exception('api-failure', assetimei2_req.text)
                        else:
                            assetimei2_req = assetimei2_req.text


                    if customer_name_req is not None :
                        if customer_name_req.status_code != 200:
                             print(customer_name_req)
                             raise Exception('api-failure', customer_name_req.text)
                        else:
                            customer_name_response = customer_name_req.text

                    if dob_response is not None :
                        if dob_response.status_code != 200:
                             print(dob_response)
                             raise Exception('api-failure', dob_response.text)
                        else:
                            dob_response = dob_response.text

                    if email_id_req is not None :
                        if email_id_req.status_code != 200:
                             print(email_id_req)
                             raise Exception('api-failure', email_id_req.text)
                        else:
                            email_id_response = email_id_req.text

                    if final_user_plan_req is not None :
                        if final_user_plan_req.status_code != 200:
                             print(final_user_plan_req)
                             raise Exception('api-failure', final_user_plan_req.text)
                        else:
                            final_user_plan_response = final_user_plan_req.text
                    


                    if phone_no_req is not None :
                        if phone_no_req.status_code != 200:
                             print(phone_no_req)
                             raise Exception('api-failure', phone_no_req.text)
                        else:
                            phone_no_response = phone_no_req.text


                    if policy_no_req is not None :
                        if policy_no_req.status_code != 200:
                             print(policy_no_req)
                             raise Exception('api-failure', policy_no_req.text)
                        else:
                            policy_no_response = policy_no_req.text
                    

                    doc = {
                                "assetimei1":assetimei1_response,
                                "assetimei2":assetimei2_response,
                                "customer_name":customer_name_response,
                                "dob":dob_response,
                                "email_id":email_id_response,
                                "final_user_plan":final_user_plan_response,
                                "phone_no":phone_no_response,
                                "policy_no":policy_no_response
                             }
                        
                    doc['insurance_id'] = 0 if record[0] is None  else record[0] 
                    doc['user_id'] = 0 if record[1] is None  else record[1] 
                    doc['date_created'] = "" if record[2] is None else record[2] 
                    db.decrypted_oppo_kash_insurance_service_user.insert_one(doc)

        except Error as e:
            print(e)
        
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   





