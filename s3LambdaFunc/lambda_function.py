import json
import boto3
import mysql.connector

cnx = mysql.connector.connect(
    user='admin',
    password='asdqwe123',
    host='movie-sentimient-analyzer.cb2xvi9ppxkq.us-west-2.rds.amazonaws.com',
    database='moviesentiment',
)
cnx.autocommit = True

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file_name = event['Records'][0]['s3']['object']['key']
    csv_object = s3_client.get_object(Bucket=bucket,Key=csv_file_name)
    file_data = csv_object['Body'].read().decode("utf-8")
    
    id_movie = csv_file_name.split('.')[0]
    
    lines = file_data.split("\n")
    for idx in range(1, len(lines)):
        cleanLine = lines[idx].strip()
        if cleanLine == "": continue
        fields = cleanLine.split(',')
        if len(fields) != 3: continue
        title = fields[0].strip().replace('"', '')
        content = fields[1].strip().replace('"', '')
        register_date = fields[2].strip().replace('"', '')
        # RDS operations
        cursor = cnx.cursor()
        cursor.execute('INSERT INTO comments (title, content, register_date, id_movie, id_sentiment) VALUES (%s, %s, %s, %s, %s)',
                           (title, content, register_date, id_movie, 3))
        cursor.close()
        
    return {
        'statusCode': 200,
        'body': json.dumps('Insertion success Lambda!')
    }
