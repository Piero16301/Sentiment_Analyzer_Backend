import mysql.connector
import glob
import csv

cnx = mysql.connector.connect(
    user='admin',
    password='asdqwe123',
    host='movie-sentimient-analyzer.cb2xvi9ppxkq.us-west-2.rds.amazonaws.com',
    database='moviesentiment',
)
cnx.autocommit = True

# CREATE TABLE IF NOT EXISTS movies (
#     id VARCHAR(8) PRIMARY KEY,
#     title VARCHAR(100) NOT NULL,
#     overview VARCHAR(1000) NOT NULL,
#     poster VARCHAR(1000) NOT NULL,
#     year INTEGER NOT NULL
# );

# CREATE TABLE IF NOT EXISTS sentiments (
#     id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
#     sentiment VARCHAR(50) NOT NULL
# );

# CREATE TABLE IF NOT EXISTS comments (
#     id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
#     title VARCHAR(200) NOT NULL,
#     content VARCHAR(2000) NOT NULL,
#     register_date VARCHAR(50) NOT NULL,
#     id_movie VARCHAR(8) NOT NULL,
#     id_sentiment INT NOT NULL,
#     FOREIGN KEY (id_movie) REFERENCES movies(id),
#     FOREIGN KEY (id_sentiment) REFERENCES sentiments(id)
# );

# with open('top250.csv', 'r', encoding='UTF8') as f:
#     reader = csv.reader(f)
#     next(reader)
#     for row in reader:
#         cursor = cnx.cursor()
#         cursor.execute('INSERT INTO movies (id, title, overview, poster, year) VALUES (%s, %s, %s, %s, %s)',
#                        (row[0], row[1], row[2], row[3], row[4]))
#         cursor.close()
#
# sentiments = ['positive', 'negative', 'neutral']
# for sentiment in sentiments:
#     cursor = cnx.cursor()
#     cursor.execute('INSERT INTO sentiments (sentiment) VALUES (%s)', (sentiment,))
#     cursor.close()

movie_counter = 1
for filename in glob.glob('data/*.csv'):
    with open(filename, 'r', encoding='UTF8') as f:
        reader = csv.reader(f)
        next(reader)
        comment_counter = 0
        for row in reader:
            cursor = cnx.cursor()
            cursor.execute('INSERT INTO comments (content, id_movie, id_sentiment) VALUES (%s, %s, %s)',
                           (row[1], filename.split('\\')[1].split('.')[0], 3))
            cursor.close()
            comment_counter += 1
        print('Added {} comments for movie {}'.format(comment_counter, movie_counter))
        movie_counter += 1
