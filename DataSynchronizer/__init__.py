import os
import yaml
import datetime
import logging
import requests
import json
#import csv
#from pathlib import Path
import azure.functions as func
from azure.storage.blob import ContainerClient, BlobServiceClient
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
    parsed_data = []
    try:
        response = requests.get(
            'https://randomuser.me/api/?results=5',
            headers={'Accept': 'application/json'},
        )
        if response.ok:
            data = response.json()
            for item in data['results']:
                parsed_data.append({
                    'name': f"{item['name']['title']}. {item['name']['first']} {item['name']['last']}",
                    'country': item['location']['country'],
                    'gender': item['gender'],
                    'email': item['email']
                })
    except Exception as e:
        print(e)

    return parsed_data

def upload(files, connection_string, container_name, timestamp=0):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print("Uploading to blob storage")

    for file in files:
        blob_client = container_client.get_blob_client(attach_timestamp(file.name,timestamp))
        with open(file.path,"rb") as data:
            blob_client.upload_blob(data)
            os.remove(file)

def upload_json_to_ABS(data, connection_string, container_name, timestamp=0):
    json_body = json.dumps(data)

    blob_service_client  = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name,blob=f"data.json")

    blob_client.delete_blob()
    blob_client.upload_blob(json_body)
    
    logging.info('File loaded to Azure Successfully...')

def restart_app(config):
    url = config['restart_url'].format(
        subscriptionId='e6e15f30-6109-4156-acf6-34e1188f268e',
        resourceGroupName='az-svk-rg',
        name='sample-nlp')
    try:
        requests.post(
            url,
            headers={'Accept': 'application/json'},
            data={}
        )   
    except Exception as e:
        logging.error(e)

def main(mytimer: func.TimerRequest) -> None:
    
    #Get Timestamp
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    
    #Get Config
    config = load_config()

    #fetch the json data
    data = fetch_n_store_data()
    
    #upload to ABS
    upload_json_to_ABS(data, config['azure_storage_conn_str'], config['container_name'], utc_timestamp)

    #restart SAMPLE NLP APP
    restart_app(config)


    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
