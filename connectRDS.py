import mysql.connector

cnx = mysql.connector.connect(
    user='admin',
    password='asdqwe123',
    host='movie-sentimient-analyzer.cb2xvi9ppxkq.us-west-2.rds.amazonaws.com',
    database='moviesentiment',
)

cnx.close()

cnx.autocommit = True
cursor = cnx.cursor()

table_create_query = """CREATE TABLE IF NOT EXISTS movies (
    id VARCHAR(8) PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    overview VARCHAR(1000) NOT NULL,
    poster VARCHAR(1000) NOT NULL,
    year INTEGER NOT NULL
);"""

cursor.execute(table_create_query)
