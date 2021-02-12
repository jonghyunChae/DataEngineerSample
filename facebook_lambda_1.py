import json
import sys
sys.path.append('./libs')
import logging
#import requests
import pymysql
from enum import Enum
import fb_bot

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

        logger.info(messaging)

        artist_name = messaging['message']['text']
        
        ## 만약에 아티스트가 없을 시에는 아티스트 추가
        query = "SELECT genre FROM production.artists t1 JOIN production.artist_genres t2 ON t2.artist_id = t1.id WHERE t1.name = '{}'".format(artist_name)
        #logger.info("Query = {0}".format(query))
        cursor.execute(query)

        genres = []
        for (genre, ) in cursor.fetchall():
            genres.append(genre)
        text = "Here are genres of {}".format(artist_name)
        bot.send_text(user_id, text)
        bot.send_text(user_id, ' ,'.join(genres))

        query = "SELECT image_url, url FROM production.artists WHERE name ='{}'".format(artist_name)
        cursor.execute(query)

        image_url, url = cursor.fetchall()[0]
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
        ## 오타 및 아티스트가 아닐 경우

        return None