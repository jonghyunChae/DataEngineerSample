import sys
import os
import logging
import pymysql
import boto3
import time
import math

client_id = "74cbd487458843f1ad3f5fa1e914c02f"
client_secret = "752e4ed11062473f9da9076c4499d51b"
#bts_id = '3Nrfpe0tUJi4K4DXYWgMUX'

host = "jongs.cli2moxtbkcd.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "production"
username = "admin"
password = "01045819402"

def main():
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("could not connect to RDS")
        sys.exit(1)

    athena = boto3.client('athena')


def query_athena(query, athena):
    # athena는 presto 기반의 빅데이터 처리 하는 시스템이기 때문에 쿼리가 mysql보다 빠를 순 없다.
    # 그 외 다양한 이유들 때문에 구조가 조금 다름
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'production'
        },
        ResultConfiguration={
            'OutputLocation': "s3://athena-panomix-tables/repair/",
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

    return response

def get_query_result(query_id, athena):
    # athena는 기본적으로 async 처리
    response = athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )

    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(5)
        response = athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )
    # 실제로 다룰 땐 MaxResults를 조절하게 됨. 이부분을 신경 쓰게 됨
    response = athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000
    )

    return response

def process_data(results):

    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    listed_results = []
    # 맨 위는 컬럼 정보
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))
        listed_results.append(dict(zip(columns, values)))

    return listed_results

def insert_row(cursor, data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values()) * 2)
    return sql


if __name__ == '__main__':
    main()