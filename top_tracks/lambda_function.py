import sys
sys.path.append('./libs')

import os
import requests # lambda에서 import 할 수 있도록
#pip3 install -r requirements.txt -t ./libs
import base64
import json
import logging
import boto3

client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"
#bts_id = '3Nrfpe0tUJi4K4DXYWgMUX'

host = "jongs.cli2moxtbkcd.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "production"
username = "admin"
password = "01045819402"

# lambda 에서는 위에서 선언
try:
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
except:
    logging.error('could not connect to dynamodb')
    sys.exit(1)

def lambda_handler(event, context):
    headers = get_headers()
    print('connect success')

    table = dynamodb.Table('top_tracks')
    artist_id = event['artist_id']

    URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
    params = {
        'country': 'US'
    }
    r = requests.get(URL, params=params, headers=headers)

    raw = json.loads(r.text)

    for track in raw['tracks']:
        data = {
            'artist_id' : artist_id
        }
        data.update(track)
        table.put_item(
            Item=data
        )

    return "SUCCESS"



def get_headers() :
    endpoints = 'https://accounts.spotify.com/api/token'
    #python 3부터는 인코딩 한번 거쳐야함
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }
    payload = {
        "grant_type": "client_credentials"
    }

    r = None
    try:
        r = requests.post(endpoints, data=payload, headers=headers)
    except :
        logging.error(r.text)
        sys.exit(1)

    error_handle(r)

    access_token = json.loads(r.text)['access_token']

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    return headers



def error_handle(r) :
    if r.status_code != 200 :
        logging.error(r.text)

        if r.status_code == 429 :
            retry_after = json.loads(r.headers)['Retry-After']
            time.sleep(int(retry_after))
        # access token expired
        elif r.status_code == 401 :
            headers = get_headers()
            pass
        else :
            sys.exit(1)
