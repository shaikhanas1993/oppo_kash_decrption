

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
            
            last_mysql_id = r.get('oppo_kash_routing_partner_last_mysql_id')
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select id,userid,date_created,email_id,first_name,lastname,mobile,personal_address from cmsservice.routing_partner where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select id,userid,date_created,email_id,first_name,lastname,mobile,personal_address from cmsservice.routing_partner  order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppo_kash_routing_partner_last_mysql_id', last_record[0])

              
                email_id_req = None
                first_name_req = None
                lastname_req = None
                mobile_req = None
                personal_address_req = None
                
                
                for record in records:
                    time.sleep(1)
                    if record[3] is not None:
                        email_id_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[3]},verify=False)
                    if record[4] is not None:
                        first_name_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[4]},verify=False)
                    if record[5] is not None:
                        lastname_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[5]},verify=False)
                    if record[6] is not None:
                        mobile_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[6]},verify=False)
                    if record[7] is not None:
                        personal_address_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[7]},verify=False)

                    
                    
                    email_id_response = ""
                    first_name_response = ""
                    lastname_response = ""
                    mobile_response = ""
                    personal_address_response = ""
                  
                    

                    if email_id_req is not None :
                        if email_id_req.status_code != 200:
                             print(email_id_req)
                             raise Exception('api-failure', email_id_req.text)
                        else:
                            email_id_response = email_id_req.text


                    if first_name_req is not None :
                        if first_name_req.status_code != 200:
                             print(first_name_req)
                             raise Exception('api-failure', first_name_req.text)
                        else:
                            first_name_response = first_name_req.text


                    if lastname_req is not None :
                        if lastname_req.status_code != 200:
                             print(lastname_req)
                             raise Exception('api-failure', lastname_req.text)
                        else:
                            lastname_response = lastname_req.text

                    if mobile_req is not None :
                        if mobile_req.status_code != 200:
                             print(mobile_req)
                             raise Exception('api-failure', mobile_req.text)
                        else:
                            mobile_response = mobile_req.text

                    if personal_address_req is not None :
                        if personal_address_req.status_code != 200:
                             print(personal_address_req)
                             raise Exception('api-failure', personal_address_req.text)
                        else:
                            personal_address_response = personal_address_req.text

                   
                    

                    doc = {
                                "email_id":email_id_response,
                                "first_name":first_name_response,
                                "lastname":lastname_response,
                                "mobile":mobile_response,
                                "personal_address":personal_address_response
                                
                             }
                        
                    doc['routing_id'] = 0 if record[0] is None  else record[0] 
                    doc['user_id'] = 0 if record[1] is None  else record[1] 
                    doc['date_created'] = "" if record[2] is None else record[2] 
                    db.decrypted_oppo_kash_routing_partner.insert_one(doc)

        except Error as e:
            print(e)
        
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   







