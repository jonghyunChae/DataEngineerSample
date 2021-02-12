import sys

import boto3

sys.path.append('./libs')
import logging
#import requests
import pymysql
import fb_bot
import base64
import json

import time
import requests

headers = None

client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"
#bts_id = '3Nrfpe0tUJi4K4DXYWgMUX'

host = "jongs.cli2moxtbkcd.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "production"
username = "admin"
password = "01045819402"

PAGE_TOKEN = "EAADtxxiAtmoBAItYNxkK0Sev0cZAAkSH7izcL7l1EwaHJev8cZCPp4sRLZC40LU2LiAfvyqyQsO8KT3csrGyPi3wAgpWVmMFHZBSdnpbOCYoYyfXJiEeC8y5ZCz6RR3BVCbX5wOZA7Y3MaHIpldAIOmB5X9MzNCWVZCFD73yh9oURZAbYsDlF4YZB"
VERIFY_TOKEN = "verify_123"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
    cursor = conn.cursor()
    logger.info("load rds success")
except :
    logger.error("could not connect to RDS Unknown Error", exc_info=True)
    sys.exit(1)

logger.info("Bot Create")
bot = fb_bot.Bot(PAGE_TOKEN)

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
        r = requests.post(endpoints, data=payload, headers=headers, timeout=2)
    except :
        logging.error(r)
        sys.exit(1)

    if r == None:
        logging.error("Get Header Timed out.")
        sys.exit(1)

    error_handle(r)

    access_token = json.loads(r.text)['access_token']

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    return headers


def error_handle(r) :
    global headers
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

def insert_row(cursor, data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values()) * 2)
    return sql

def invoke_lambda(fxn_name, payload, invocation_type = 'Event') :
    lambda_client = boto3.client('lambda')
    invoke_response = lambda_client.invoke(
        FunctionName = fxn_name,
        InvocationType = invocation_type,
        Payload = json.dumps(payload)
    )

    if invoke_response['StatusCode'] not in [200, 202, 204] :
        logging.error("ERROR : Invoking lambda function : '{0}' failed".format(fxn_name))
    return invoke_response

def search_artist(artist_name, limit = '1') :
    global headers
    headers = get_headers()
    logger.info('get header sucess')

    params = {
        'q' : artist_name,
        'type' : 'artist',
        'limit' : limit
    }

    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
    logger.info('query artist by api success')
    raw = json.loads(r.text)
    if r.status_code == 400 :
        # 람다의 경우 aws에서 오픈 확인해야 함
        logger.info('StatusCode is 400 {}'.format(raw))
        return "StatusCode is 400"


    logger.info('api results : {}'.format(raw))
    if raw['artists']['items'] == []:
        return "Could not find artist. Please Try again!"
    artist = {}

    artist_raw = raw['artists']['items'][0]
    if artist_raw['name'] == artist_name:
        artist.update({
            'id': artist_raw['id'],
            'name': artist_raw['name'],
            'followers': artist_raw['followers']['total'],
            'popularity': artist_raw['popularity'],
            'url': artist_raw['external_urls']['spotify'],
            'image_url': artist_raw['images'][0]['url']
        })

        new_genres = artist_raw['genres']
        if len(new_genres) != 0 :
            for i in new_genres :
                logger.info('added genres {}'.format(i))
                insert_row(cursor, {'artist_id' : artist_raw['id'], 'genre':i }, 'artist_genres')

        logger.info(insert_row(cursor, artist, 'artists'))
        conn.commit()
        r = invoke_lambda('jongs-spotify', payload={'artist_id' : artist_raw['id']})
        print(r)


        return "We added artist. Please try again in a second!"

    return "Unknown Errors"



def lambda_handler(event, context):
    if 'params' in event.keys() :
        if event['params']['querystring']['hub.verify_token'] == VERIFY_TOKEN:
            return int(event['params']['querystring']['hub.challenge'])
        else:
            logger.error('wrong validation token')
            raise SystemExit
    else:
        messaging = event['entry'][0]['messaging'][0]
        user_id = messaging['sender']['id']

        '''
        #logger.info(messaging)
        {'message': {'attachment': {'type': 'template', 'payload': {'template_type': 'generic', 'elements': [{'title': "Artist Info: 'maroon 5'", 'image_url': 'https://i.scdn.co/image/305c7c493ca5c108ade08dea6a169694db911644', 'subtitle': 'information', 'default_action': {'type': 'web_url', 'url': 'https://open.spotify.com/artist/04gDigrS5kc9YWfZHwBETP', 'webview_height_ratio': 'full'}}]}}}, 'recipient': {'id': '3657773467679089'}, 'messaging_type': 'RESPONSE'}
        '''

        artist_name = messaging['message']['text']

        query = "SELECT image_url, url FROM production.artists WHERE name ='{}' limit 1".format(artist_name)
        cursor.execute(query)
        raw = cursor.fetchall()

        if len(raw) == 0:
            logger.info("search_artist : {}".format(artist_name))
            text = search_artist(cursor, artist_name)
            bot.send_text(user_id, text)
            sys.exit(0)

        image_url, url = raw[0]

        logger.info("ImageURL = {0}, URL = {1}".format(image_url, url))
        payload = {
            'template_type' : 'generic',
            'elements' : [{
                "title" : "Artist Info: '{}'".format(artist_name),
                'image_url' : image_url,
                'subtitle' : 'information',
                'default_action' : {
                    'type' : 'web_url',
                    'url' : url,
                    'webview_height_ratio' : 'full'
                }
            }]
        }
        bot.send_attachment(user_id, "template", payload)

        query = "SELECT genre FROM production.artists t1 JOIN production.artist_genres t2 ON t2.artist_id = t1.id WHERE t1.name = '{}'".format(artist_name)
        #logger.info("Query = {0}".format(query))
        cursor.execute(query)


        genres = []
        for (genre, ) in cursor.fetchall():
            genres.append(genre)
        text = "Here are genres of {}".format(artist_name)
        bot.send_text(user_id, text)

        bot.send_text(user_id, ' ,'.join(genres))

        ## 만약에 아티스트가 없을 시에는 아티스트 추가
        ## spotify API hit -> artist search

        ## database upload

        ## one second

        ## 오타 및 아티스트가 아닐 경우

        return None
