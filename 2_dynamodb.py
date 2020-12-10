import sys
import time

import np
import base64
import json
import logging
import requests
import pymysql
import boto3
from boto3.dynamodb.conditions import Key, Attr

client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"
#bts_id = '3Nrfpe0tUJi4K4DXYWgMUX'
headers = None

host = "jongs.cli2moxtbkcd.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "production"
username = "admin"
password = "01045819402"


def main():
    global headers
    try:
        # credential 세팅 떄문인가 region 적으면 안되네..
        dynamodb = boto3.resource('dynamodb', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)
    print('connect success')

    table = dynamodb.Table('top_tracks')
    '''
    response = table.get_item(
        Key={
            'artist_id' : '03r4iKL2g2442PT9n2UKsx',
            'id' : '5NLuC70kZQv8q34QyQa1DP',
        }
    )
    '''
    """
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('artist_id').eq('03r4iKL2g2442PT9n2UKsx'),
        FilterExpression=boto3.dynamodb.conditions.Attr('popularity').gt(70),
    )
    """

    response = table.scan(
        #KeyConditionExpression=boto3.dynamodb.conditions.Key('artist_id').eq('03r4iKL2g2442PT9n2UKsx'),
        FilterExpression=boto3.dynamodb.conditions.Attr('popularity').gt(90),
    )


    print(len(response['Items']))
    print(response['Items'])



def dynamo_write():
    global headers
    try:
        # region 적으면 안되네..
        dynamodb = boto3.resource('dynamodb', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)
    print('connect success')

    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("could not connect to RDS")
        sys.exit(1)

    headers = get_headers()

    table = dynamodb.Table('top_tracks')
    #table.put_item(Item={'artist_id' : 1})
    cursor.execute('SELECT id FROM artists')

    countries = ['US', 'CA']
    for country in countries:
        for (artist_id, ) in cursor.fetchall():
            URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
            params = {
                'country' : country
            }

            r = requests.get(URL, params=params, headers=headers)

            raw = json.loads(r.text)
            datas = []
            for track in raw['tracks'] :
                data = {
                    'artist_id' : artist_id,
                    'country' : country
                }
                data.update(track)
                datas.append(data)
                #table.put_item(Item=data)

            with table.batch_writer() as batch:
                for item in datas :
                    batch.put_item(Item=item)

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

if __name__ == '__main__':
    main()