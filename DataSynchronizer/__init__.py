import os
import yaml
import datetime
import logging
import requests
#import json
import csv
from pathlib import Path

import azure.functions as func
from azure.storage.blob import ContainerClient
#from azure.identity import DefaultAzureCredential

dir_root = os.path.dirname(os.path.abspath(__file__))
parent_root = os.path.dirname(dir_root)

def attach_timestamp(filename, timestamp=0):
    namelist = filename.split('.')
    namelist[0] = f'{namelist[0]}_{timestamp}'
    return '.'.join(namelist)

def load_config():
    with open(dir_root + "/config.yaml","r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def get_files(dir):
    with os.scandir(dir) as entries:
        for entry in entries:
            if entry.is_file() and not entry.name.startswith('.'):
                yield entry

def fetch_n_store_data():
    sitecoreData = []
    # while number <= 11:
    try:
    # api_url = f"https://cdt.maersk.com/api_sc9/faqs?limit=30&page={number}&orderby=priority,name"
    # headers = {
    #     'Accept': 'application/json', 
    #     'sc_apikey': '7B082D90-25F3-4D28-AF21-5BDD912DC5B1',
    #     'Cookie':"_abck=7016021EB22821CB246C83995FA03CA1~-1~YAAQJIgsMW5CTwt8AQAAc1hnEQZOewyac9KNEkHQdX8DXnb8S87CiHCRpAenL89zxzkJe9FtjunsLizYXktmVxyY9Q3PC8oacXf35fiOKLiNX8ZoeGlnjsAFsTmZuacRyn8Hu4xdgDb7szhAuj5UJ5WVU7w4KRLGP1i2dN1sAoORN3AJZkZZpJiGgZZTL2mCWCrcHBVQAxijhJ+iS46zL/CToMKfuYmMcUKQvQVoBi6uxqLT0wIW0Tc0u/QwM4Ckh6nh+Ak4xzTWcm2UOfptsgh3M3Gc+4DNXojNq9UtMMIz7Ovy0qb1Aji4kqduxssCvvntMUgLSIYxQu8ZdbkdRYFL1r2nQ7zNhPqzUhnPt4oXyrFiTNmVDkQ=~-1~-1~-1; ak_bmsc=75DF12346D33CE60A4820511160EFF92~000000000000000000000000000000~YAAQJIgsMW9CTwt8AQAAc1hnEQ25GMHm96qtS0pG7eVDfCtZUDhXEChG78SMk4h6eLt5ayl5JtPTp1xfzz6KqBB0I3avdb618WuYbeZQt2zSyMoDpmq65QqngxNnxitj4QiMdy27qirV5W7lcVutInpLIEqRCbRfC2ULRer1GIp4OvoLXbwr/vzCCLvEin6dobsTqNMtiZNIM7hrNq9ODmoE330c0/YuGYp/0WmQcrAtJiKLOHwgUEINkRLgiZMm40bNCOWboO7KyhpGQBkdswUsE72H/O9r3uKzUbreOxVnHzmuVAF/hno7lvrXH2GMvhCRwVMnnv52r7Jv0gcl4DAlFul9S+jPlmfkJZqovWM3gMG8PZmxH/cDg5s=; bm_sz=3FB43CC0AE1B6964CEA8CBA72B3A367B~YAAQJIgsMXBCTwt8AQAAc1hnEQ1TwjfLHM49O1CHvKk0PyDCr9A28t18vMfRmVM0nHFprr+swr1/mBu56gRANlRwkjHMa+IdgyK6oZVGL91Mb2nC1A9yVGBVMQKMjy5EGR00zRKEJQIipvua3iynDFJ8oUrHsnyI0mlRHDtoAWilHu3WzmsFlj/jHUT6bSfl6dGRsD9DjSu0sNCoOXu1fds0SECD/0Zpucg9nmaYKI2tXUcz/UZ+M/n1CRiU0axEf4mET+LE4UJ/MMiQ4pk60gQsS8r9mQbCVV6GbJD9/fGGGj4=~3421233~3224625; utmcam=EMPTY; utmcon=EMPTY; utmmed=EMPTY; utmsrc=EMPTY; utmter=EMPTY; country_code=in; maersk#lang=en"
    #     }
        response = requests.get(
            'https://randomuser.me/api/?results=5',
            headers={'Accept': 'application/json'},
        )
        if response.ok:
            data = response.json()

            # for list_items in data['data']['results']:
            #     sitecoreData.append(list_items)

            for item in data['results']:
                sitecoreData.append({
                    'name': f"{item['name']['title']}. {item['name']['first']} {item['name']['last']}",
                    'country': item['location']['country'],
                    'gender': item['gender'],
                    'email': item['email']
                })
    #number = number + 1
    except Exception as e:
        print(e)

    ##### CSV Parsing - START #########
    path = parent_root + "/tmp/users.csv"
    data_file = open(path, 'w', newline='')
    csv_writer = csv.writer(data_file)

    count = 0
    for emp in sitecoreData:
        if count == 0:
            # Writing headers of CSV file
            header = emp.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(emp.values())
    ##### CSV Parsing - END #########

def upload(files, connection_string, container_name, timestamp=0):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print("Uploading to blob storage")

    for file in files:
        blob_client = container_client.get_blob_client(attach_timestamp(file.name,timestamp))
        with open(file.path,"rb") as data:
            blob_client.upload_blob(data)
            os.remove(file)

def main(mytimer: func.TimerRequest) -> None:
    
    #Get Timestamp
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    
    #Get Config
    config = load_config()

    #fetch the json data and store
    fetch_n_store_data()
    
    #upload to ABS
    upload(get_files(parent_root + "/tmp"), config['azure_storage_conn_str'], config['container_name'], utc_timestamp)

    logging.info('File loaded to Azure Successfully...')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
