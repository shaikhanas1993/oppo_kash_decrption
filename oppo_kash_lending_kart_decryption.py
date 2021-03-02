

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
            
            last_mysql_id = r.get('oppo_kash_lending_kart_loan_application_last_mysql_id')
            query = ""
            if last_mysql_id is not None:
                last_mysql_id = int(last_mysql_id)
                query = f"select id,user_id,date_created,business_run_by,bussiness_address,company_name,email,emi_details,first_name,gender,last_name,loan_bank_account_details,mobile,net_income,personal_address,personalpan from lendingcartservice.lendingcart_loanapplication where id > {last_mysql_id} order by id Asc limit 1000"
            else:
                query = f"select id,user_id,date_created,business_run_by,bussiness_address,company_name,email,emi_details,first_name,gender,last_name,loan_bank_account_details,mobile,net_income,personal_address,personalpan from lendingcartservice.lendingcart_loanapplication order by id Asc  limit 1000"
            print(query)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            if(len(records) == 0):
                pass
            else:
                last_record = records[-1]
                r.set('oppo_kash_lending_kart_loan_application_last_mysql_id', last_record[0])

              
                business_run_by_req = None
                bussiness_address_req = None
                company_name_req = None
                email_req = None
                emi_details_req = None
                first_name_req = None
                gender_req = None
                last_name_req = None
                loan_bank_account_details_req = None
                mobile_req = None
                net_income_req = None
                personal_address_req = None
                personalpan_req = None
                
                for record in records:
                    time.sleep(1)
                    if record[3] is not None:
                        business_run_by_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[3]},verify=False)
                    if record[4] is not None:
                        bussiness_address_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[4]},verify=False)
                    if record[5] is not None:
                        company_name_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[5]},verify=False)
                    if record[6] is not None:
                        email_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[6]},verify=False)
                    if record[7] is not None:
                        emi_details_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[7]},verify=False)

                    if record[8] is not None:
                        first_name_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[8]},verify=False)

                    if record[9] is not None:
                        gender_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[9]},verify=False)

                    if record[10] is not None:
                        last_name_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[10]},verify=False)

                    if record[11] is not None:
                        loan_bank_account_details_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[11]},verify=False)

                    if record[12] is not None:
                        mobile_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[12]},verify=False)

                    if record[13] is not None:
                        net_income_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[13]},verify=False)

                    if record[14] is not None:
                        personal_address_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[14]},verify=False)

                    if record[15] is not None:
                        personalpan_req = requests.post(url, auth=HTTPBasicAuth('realmeuser', 'encryptDecrypt#P@ssw0rd'),headers=headers,json = {'attribute':record[15]},verify=False)

                    
                    
                    business_run_by_response = ""
                    bussiness_address_response = ""
                    company_name_response = ""
                    email_response = ""
                    emi_details_response = ""
                    first_name_response = ""
                    gender_response = ""
                    last_name_response = ""
                    loan_bank_account_details_response = ""
                    mobile_response = ""
                    net_income_response = ""
                    personal_address_response = ""
                    personalpan_response = ""
                  
                    

                    if business_run_by_req  is not None :
                        if business_run_by_req .status_code != 200:
                             print(business_run_by_req )
                             raise Exception('api-failure', business_run_by_req .text)
                        else:
                            business_run_by_response = business_run_by_req.text


                    if bussiness_address_req   is not None :
                        if bussiness_address_req .status_code != 200:
                             print(bussiness_address_req )
                             raise Exception('api-failure', bussiness_address_req.text)
                        else:
                            bussiness_address_response = bussiness_address_req.text


                    if company_name_req is not None :
                        if company_name_req.status_code != 200:
                             print(company_name_req)
                             raise Exception('api-failure', company_name_req.text)
                        else:
                            company_name_response = company_name_req.text

                    if email_req is not None :
                        if email_req.status_code != 200:
                             print(email_req)
                             raise Exception('api-failure', email_req.text)
                        else:
                            email_response = email_req.text

                    if emi_details_req  is not None :
                        if emi_details_req.status_code != 200:
                             print(emi_details_req )
                             raise Exception('api-failure', emi_details_req.text)
                        else:
                            emi_details_response  = emi_details_req.text

                    if first_name_req  is not None :
                        if first_name_req.status_code != 200:
                             print(first_name_req )
                             raise Exception('api-failure', first_name_req.text)
                        else:
                            first_name_response  = first_name_req.text


                    if gender_req  is not None :
                        if gender_req.status_code != 200:
                             print(gender_req )
                             raise Exception('api-failure', gender_req.text)
                        else:
                            gender_response  = gender_req.text


                    if last_name_req  is not None :
                        if last_name_req.status_code != 200:
                             print(last_name_req )
                             raise Exception('api-failure', last_name_req.text)
                        else:
                            last_name_response  = last_name_req.text


                    if loan_bank_account_details_req  is not None :
                        if loan_bank_account_details_req.status_code != 200:
                             print(loan_bank_account_details_req )
                             raise Exception('api-failure', loan_bank_account_details_req.text)
                        else:
                            loan_bank_account_details_response  = loan_bank_account_details_req.text

                    if mobile_req  is not None :
                        if mobile_req.status_code != 200:
                             print(mobile_req )
                             raise Exception('api-failure', mobile_req.text)
                        else:
                            mobile_response  = mobile_req.text

                    if net_income_req  is not None :
                        if net_income_req.status_code != 200:
                             print(net_income_req )
                             raise Exception('api-failure', net_income_req.text)
                        else:
                            net_income_response  = net_income_req.text

                    if personal_address_req  is not None :
                        if personal_address_req.status_code != 200:
                             print(personal_address_req )
                             raise Exception('api-failure', personal_address_req.text)
                        else:
                            personal_address_response  = personal_address_req.text

                    if personalpan_req  is not None :
                        if personalpan_req.status_code != 200:
                             print(personalpan_req )
                             raise Exception('api-failure', personalpan_req.text)
                        else:
                            personalpan_response  = personalpan_req.text
                   
                    

                    doc = {
                                "business_run_by":business_run_by_response,
                                "bussiness_address":bussiness_address_response,
                                "company_name":company_name_response,
                                "email":email_response,
                                "emi_details":emi_details_response,
                                "first_name":first_name_response, 
                                "gender":gender_response,
                                "last_name":last_name_response,
                                "loan_bank_account_details":loan_bank_account_details_response,
                                "mobile":mobile_response,
                                "net_income":net_income_response ,
                                "personal_address":personal_address_response, 
                                "personalpan":personalpan_response 
                             }
                        
                    doc['routing_id'] = 0 if record[0] is None  else record[0] 
                    doc['user_id'] = 0 if record[1] is None  else record[1] 
                    doc['date_created'] = "" if record[2] is None else record[2] 
                    db.decrypted_oppo_kash_lending_kart_loan_application.insert_one(doc)

        except Error as e:
            print(e)
        
       
except Error as e:
    logger.critical("Error while connecting to MySQL", e)
    print(e)
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")   








