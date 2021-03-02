

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
            
            last_mysql_id = r.get('oppo_kash_sigupservice_common_user_last_mysql_id')
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select id,date_created,commemail,fname,lname,pannumber,profilepicstoragepath,profilepicurl from signupservice.common_user where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select id,date_created,commemail,fname,lname,pannumber,profilepicstoragepath,profilepicurl from signupservice.common_user  order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppo_kash_sigupservice_common_user_last_mysql_id', last_record[0])

              
                commemail_req = None
                fname_req = None
                lname_req = None
                pannumber_req = None
                profilepicstoragepath_req = None
                profilepicurl_req = None
               
                
                for record in records:
                    time.sleep(1)
                    if record[2] is not None:
                        commemail_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[2]},verify=False)
                    if record[3] is not None:
                        fname_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[3]},verify=False)
                    if record[4] is not None:
                        lname_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[4]},verify=False)
                    if record[5] is not None:
                        pannumber_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[5]},verify=False)
                    if record[6] is not None:
                        profilepicstoragepath_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[6]},verify=False)
                  
                    if record[7] is not None:
                        profilepicurl_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[7]},verify=False)
                   
                    
                    commemail_response = ""
                    fname_response = ""
                    lname_response = ""
                    pannumber_response = ""
                    profilepicstoragepath_response = ""
                    profilepicurl_response = ""
                    

                    if commemail_req is not None :
                        if commemail_req.status_code != 200:
                             print(commemail_req)
                             raise Exception('api-failure', commemail_req.text)
                        else:
                            commemail_response = commemail_req.text
                    if fname_req is not None :
                        if fname_req.status_code != 200:
                             print(fname_req)
                             raise Exception('api-failure', fname_req.text)
                        else:
                            fname_response = fname_req.text
                    if lname_req is not None :
                        if lname_req.status_code != 200:
                             print(lname_req)
                             raise Exception('api-failure', lname_req.text)
                        else:
                            lname_response = lname_req.text

                    if pannumber_req is not None :
                        if pannumber_req.status_code != 200:
                             print(pannumber_req)
                             raise Exception('api-failure', pannumber_req.text)
                        else:
                            pannumber_response = pannumber_req.text

                    if profilepicstoragepath_req is not None :
                        if profilepicstoragepath_req.status_code != 200:
                             print(profilepicstoragepath_req)
                             raise Exception('api-failure', profilepicstoragepath_req.text)
                        else:
                            profilepicstoragepath_response = profilepicstoragepath_req.text

                    if profilepicurl_req is not None :
                        if profilepicurl_req.status_code != 200:
                             print(profilepicurl_req)
                             raise Exception('api-failure', profilepicurl_req.text)
                        else:
                            profilepicurl_response = profilepicurl_req.text

                    

                    doc = {
                                "commemail":commemail_response,
                                "fname":fname_response,
                                "lname":lname_response,
                                "pannumber":pannumber_response,
                                "profilepicstoragepath":profilepicstoragepath_response,
                                "profilepicurl":profilepicurl_response
                             }
                        
                    doc['id'] = 0 if record[0] is None  else record[0] 
                    doc['date_created'] = "" if record[1] is None else record[1] 
                    db.decrypted_oppo_kash_signup_service_common_user.insert_one(doc)

        except Error as e:
            print(e)
        
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   




