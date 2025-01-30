import requests
from bs4 import BeautifulSoup
import urllib.request
import mysql.connector
import xml.etree.ElementTree as ET
from mysql.connector import errorcode


req = urllib.request
d_key = 'API_KEY_FOR_PUBLIC_DATA'

def lambda_handler(event, context):
    getRTMSDataSvcAptTrade('11110', '202312', d_key)
    getRTMSDataSvcAptTrade('11350', '202312', d_key)
    


def insert_data_into_mysql(data):
	    try:
	        connection = mysql.connector.connect(
	            host='RDS_END_POINT',
	            user='pipeline',
	            password='PWD',
	            database='pipelinedb'
	        )
	
	        cursor = connection.cursor()
	
	        for row in data:
                
	            sql = """
                        INSERT INTO apart_real_cost (
                        deal_amt, build_year, deal_year, dong, apart_nm, deal_month,
                        deal_day, area_ex_use, jibun, regional_code, floor, reg_date
	                ) VALUES (
	                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s
	                )
	            """
	            values = (
	                row['dealAmount'], row['buildYear'], row['dealYear'], row['dong'],
	                row['apartmentName'], row['dealMonth'], row['dealDay'], row['areaForExclusiveUse'],
	                row['jibun'], row['regionalCode'], row['floor'], row['rgstDate']
	            )
	            cursor.execute(sql, values)
	
	        connection.commit()
	
	    except mysql.connector.Error as err:
	        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
	            print("Access denied: Check your MySQL username and password")
	        elif err.errno == errorcode.ER_BAD_DB_ERROR:
	            print("Database does not exist")
	        else:
	            print("Error: {}".format(err))
	    finally:
	        if 'connection' in locals() and connection.is_connected():
	            cursor.close()
	            connection.close()

def getRTMSDataSvcAptTrade(LAWD_CD, DEAL_YMD, serviceKey):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    url = url + "?&LAWD_CD=" + LAWD_CD
    url = url + "&DEAL_YMD=" + DEAL_YMD
    url = url + "&serviceKey=" + serviceKey

    xml = urllib.request.urlopen(url)
    result = xml.read()
    
    # Parse the XML data
    root = ET.fromstring(result)

    # Find all "item" elements
    items = root.findall(".//item")  # Use XPath to locate all "item" elements

    apt_trade_data = []
    try:
        for item in items:
            dealAmount = item.find("dealAmount").text  
            dealYear = item.find("dealYear").text  
            dong = item.find("umdNm").text  
            apartmentName = item.find("aptNm").text  
            dealMonth = item.find("dealMonth").text  
            dealDay = item.find("dealDay").text  
            areaForExclusiveUse = item.find("excluUseAr").text  
            regionalCode = item.find("sggCd").text  
            floor = item.find("floor").text  
            rgstDate = item.find("rgstDate").text  


            try:
                jibun = item.find("regionalCode").text
            except:
                jibun = ""

            try:
                buildYear = item.find("buildYear").text
            except:
                buildYear = ""

            apt_trade_data.append({
                    'dealAmount': dealAmount,
                    'buildYear': buildYear,
                    'dealYear': dealYear,
                    'dong': dong,
                    'apartmentName': apartmentName,
                    'dealMonth': dealMonth,
                    'dealDay': dealDay,
                    'areaForExclusiveUse': areaForExclusiveUse,
                    'jibun': jibun,
                    'regionalCode': regionalCode,
                    'floor': floor,
                    'rgstDate': rgstDate
                })
        insert_data_into_mysql(apt_trade_data)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied: Check your MySQL username and password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("Error: {}".format(err))
            
getRTMSDataSvcAptTrade('11110', '202312', d_key)