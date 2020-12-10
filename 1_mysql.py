import csv
import os
import sys
import time

import np
import base64
import json
import logging
import requests
import pymysql
import boto3

#import np as numpy
#client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"
bts_id = '3Nrfpe0tUJi4K4DXYWgMUX'
headers = None

host = "jongs.cli2moxtbkcd.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "production"
username = "admin"
password = "01045819402"

base_dir = os.path.dirname(os.path.abspath( __file__ ))
def main():
    global headers
    headers = get_headers()
    print(headers)

    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("could not connect to RDS")
        sys.exit(1)

    batch_process(conn, cursor)
    print('program is exited')

    return
    #cursor.execute('show tables')
    #print(cursor.fetchall())

    #query = "INSERT INTO artist_genres (artist_id, genre) VALUES ('{}', '{}')".format('2345', 'pop')
    #cursor.execute(query)
    #conn.commit()
    r = get_search('BTS', '1')
    raw = json.loads(r.text)
    print(raw['artists']['items'][0].keys())

    artist_raw = raw['artists']['items'][0]
    if artist_raw['name'] == 'BTS' :
        artist = {
            'id' : artist_raw['id'],
            'name' : artist_raw['name'],
            'followers' : artist_raw['followers']['total'],
            'popularity' : artist_raw['popularity'],
            'url' : artist_raw['external_urls']['spotify'],
            'image_url' : artist_raw['images'][0]['url']
        }
        print( insert_row(artist) )
    query = """
            INSERT INTO artists
            (`id`, `name`, `followers`, `popularity`, `url`, `image_url`)
            VALUES ('{}', '{}', {}, {}, '{}', '{}')
            ON DUPLICATE KEY UPDATE id='{}', name='{}', followers={}, popularity={},  url='{}', image_url='{}'
    """.format(
        artist['id'],
        artist['name'],
        artist['followers'],
        artist['popularity'],
        artist['url'],
        artist['image_url'],
        artist['id'],
        artist['name'],
        artist['followers'],
        artist['popularity'],
        artist['url'],
        artist['image_url'],
    )


    cursor.execute(query)
    conn.commit()

    print(query)
    sys.exit(0)


    r = get_albums(bts_id)
    raw = json.loads(r.text)
    #print(raw)
    print(raw['total'])
    print(raw['offset'])
    print(raw['limit'])
    print(raw['next'])

    albums = []
    albums.extend(raw['items'])

    #난 100개만 뽑아 오겠다.
    count = 0
    next = raw['next']
    while count < 100 and next :
        r = requests.get(next, headers=headers)
        raw = json.loads(r.text)
        next = raw['next']
        print(next)
        albums.extend(raw['items'])
        count = len(albums)
    for i in albums:
        print(i)
    print(len(albums))
    #print(r.status_code)
    #print(r.text)
    #print(r.headers)


def insert_row(cursor, data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values()) * 2)
    return sql

def get_albums(artist_id) :
    global headers
    r = requests.get("https://api.spotify.com/v1/artists/{}/albums".format(artist_id), headers=headers)
    return r

def get_search(key_word, limit = '1') :
    params = {
        'q' : key_word,
        'type' : 'artist',
        'limit' : limit
    }

    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
    return r

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


def artist_process1(conn, cursor):
    global headers

    print(base_dir)
    artists = []
    with open(base_dir + '\\artist_list.csv', encoding='UTF8') as f:
        print(f)
        raw = list(csv.reader(f))
        for row in raw:
            artists.append(row[0])

    for a in artists:
        text = get_search(a, '1').text
        raw = json.loads(text)
        artist = {}

        try:
            artist_raw = raw['artists']['items'][0];
            if artist_raw['name'] == a:
                artist.update(
                    {
                        'id': artist_raw['id'],
                        'name': artist_raw['name'],
                        'followers': artist_raw['followers']['total'],
                        'popularity': artist_raw['popularity'],
                        'url': artist_raw['external_urls']['spotify'],
                        'image_url': artist_raw['images'][0]['url']
                    }
                )
                insert_row(cursor, artist, 'artists')
                print(a)
        except:
            logging.error("NO ITEMS FROM SEARCH API")
            continue

    conn.commit()
    print('program is exited')

# api 지원 되는 경우
def batch_process(conn, cursor):
    global headers
    artists = []
    cursor.execute('SELECT id FROM artists')
    for (id, ) in cursor.fetchall():
        artists.append(id)

    artist_genres = []
    artist_batch = [artists[i: i+50] for i in range(0, len(artists), 50)]
    for i in artist_batch :
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/artists/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)

        for artist in raw['artists']:
            for genre in artist['genres']:
                artist_genres.append(
                    {
                        'artist_id' : artist['id'],
                        'genre' : genre
                    }
                )
    for data in artist_genres :
        insert_row(cursor, data, 'artist_genres')
    conn.commit()
    print('batch_process closed')
    sys.exit(0)

def dynamo_test():
    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northest-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)
    print('success')

if __name__ == '__main__':
    main()

