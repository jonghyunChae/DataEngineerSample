import sys
import os
import logging
import time

import boto3
import requests
import base64
import json
import pymysql
from datetime import datetime
import pandas as pd
import jsonpath

client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"
headers = None

host = "jongs.cli2moxtbkcd.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "production"
username = "admin"
password = "01045819402"

def main():
    global headers
    conn, cursor = mysql_connect()

    headers = get_headers()

    # RDS - 아티스트의 ID를 가져오고
    cursor.execute("SELECT id FROM artists LIMIT 10")
    top_track_keys = {
        "id" : "id",
        "name" : "name",
        "popularity" : "popularity",
        "external_url" : "external_urls.spotify"
    }

    # Top Tracks Spotify 가져오고
    top_tracks=[]
    for (id, ) in cursor.fetchall():
        r = get_top_tracks(id)
        raw = json.loads(r.text)

        for track in raw['tracks'] :
            top_track = {}
            for k, v in top_track_keys.items() :
                #키에 맞는 것 찾아서 어펜드
                top_track.update({k: jsonpath.jsonpath(track, v)})
                top_track.update({'artist_id' : id})
                top_tracks.append(top_track)

        #top_tracks.extend(raw['tracks'])

    # track_ids
    track_ids = [i['id'][0] for i in top_tracks]

    # list of dictionaries
    top_tracks = pd.DataFrame(top_tracks)
    dt = datetime.utcnow().strftime('%Y-%m-%d')
    print(dt)

    s3_upload_with_make_parquet(dataframe=top_tracks, name='top_tracks', key=dt)

    # .json 으로 했다면
    """
    with open('top_tracks.json', 'w') as f:
        for i in top_tracks :
            json.dump(i, f)
            f.write(os.linesep)

    data = open('top-tracks.json', 'rb')
    object.put(Body=data)
    """

    # S3 import
    tracks_batch = [track_ids[i: i + 100] for i in range(0, len(track_ids), 100)]

    audio_features = []

    for i in tracks_batch :
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)

        audio_features.extend(raw['audio_features'])

    audio_features = pd.DataFrame(audio_features)

    s3_upload_with_make_parquet(dataframe=audio_features, name='audio_features', key=dt)

# 스파크가 읽을 수 있는 파티션으로 저장 해야함
def s3_upload_with_make_parquet(dataframe, name, key):
    parquet_path = '{}.parquet'.format(name)
    dataframe.to_parquet(parquet_path, engine='pyarrow', compression='snappy')

    s3 = boto3.resource('s3')
    upload_path = '{}/dt={}/{}'.format(name, key, parquet_path)
    print(upload_path)
    object = s3.Object('jongs-spotify-artists', upload_path)
    data = open(parquet_path, 'rb')
    object.put(Body=data)

def mysql_connect():
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("could not connect to RDS")
        sys.exit(1)
    return conn, cursor

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

def get_top_tracks(id):
    global headers
    URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
    params = {
        'country' : 'US'
    }
    r = requests.get(URL, headers=headers, params=params)
    return r

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



if __name__ == "__main__":
    main()