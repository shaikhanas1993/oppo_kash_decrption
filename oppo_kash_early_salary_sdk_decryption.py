import time
from datetime import datetime,date
import mysql.connector
from mysql.connector import Error
import warnings
import redis
from kafka import KafkaProducer
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

headers = {'Content-type': 'application/json'}
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
            
            last_mysql_id = r.get('oppokash_earlysalary_sdk_last_mysql_id')
          
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select id,gps_data,inbox_data,package_data,sdk_device_data,sent_data,date_created,es_ref_id from earlysalaryservice.sdk_info where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select id,gps_data,inbox_data,package_data,sdk_device_data,sent_data,date_created,es_ref_id from earlysalaryservice.sdk_info  order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppokash_earlysalary_sdk_last_mysql_id', last_record[0])
                gps_data_req = None
                inbox_data_req = None
                package_data_req = None
                sdk_device_data_req = None
                sent_data_req = None
                for record in records:
                    try:
                        time.sleep(1)
                        if record[1]  is not None:
                            gps_data_req = requests.post('http://localhost:8080/decryptEarlySalarySdkApi',headers=headers,json = {'input':record[1]},verify=False)

                        if record[2] is not None:
                            inbox_data_req = requests.post('http://localhost:8080/decryptEarlySalarySdkApi',headers=headers,json = {'input':record[2]},verify=False)    
                        
                        if record[3] is not None:
                            package_data_req = requests.post('http://localhost:8080/decryptEarlySalarySdkApi',headers=headers,json = {'input':record[3]},verify=False)
                            
                        if record[4] is not None:
                            sdk_device_data_req = requests.post('http://localhost:8080/decryptEarlySalarySdkApi',headers=headers,json = {'input':record[4]},verify=False)
                            
                        if record[5] is not None:
                            sent_data_req = requests.post('http://localhost:8080/decryptEarlySalarySdkApi',headers=headers,json = {'input':record[5]},verify=False)
                        
                        
                        gps_data_response = " " if gps_data_req is  None else gps_data_req.text
                        inbox_data_response = " " if inbox_data_req is  None else inbox_data_req.text
                        package_data_response = " " if package_data_req is  None else package_data_req.text
                        sdk_device_data_response  = " " if sdk_device_data_req is  None else sdk_device_data_req.text
                        sent_data_response = " " if sent_data_req is  None else sent_data_req.text
                        
                        print(gps_data_response)
                        print(inbox_data_response)
                        print(package_data_response)
                        print(sdk_device_data_response)
                        print(sent_data_response)
                        
                        

                        doc = {
                                "gps_data":gps_data_response,
                                "inbox_data":inbox_data_response,
                                "package_data":package_data_response,
                                "sdk_device_data":sdk_device_data_response,
                                "sent_data":sent_data_response
                            }
                        
                        doc['earlysalarysdk_id'] = 0 if record[0] is None else record[0] 
                        doc['date_created'] = "" if record[6] is None else record[6] 
                        doc['es_ref_id']= 0 if record[7] is None else record[7] 
                        print(doc)
                        db.decrypted_oppo_kash_early_salary_sdk.insert_one(doc)
                    except Timeout:
                        print(f"The request timed out::record[0]") 
                        data = {'earlysalarysdk_id' : record[0]}
                        print(data)
                    
        except redis.ConnectionError:
            print("Redis connection problem")
            print(E)
        except Exception as e:
            print(e)
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   
