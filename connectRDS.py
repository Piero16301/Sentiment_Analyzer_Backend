import psycopg2
import glob
import csv

conn = psycopg2.connect(host="database-1.cjkzjtw15olq.us-east-1.rds.amazonaws.com",
                                database="sentimentdb",
                                user="postgres",
                                password="postgres",
                                )
        conn.autocommit = True


## Call Lambda after insert -----------------------------------------
### Create Func
"""
CREATE FUNCTION call_lambda() RETURNS trigger AS $$
begin
	PERFORM * from aws_lambda.invoke(aws_commons.create_lambda_function_arn('arn:aws:lambda:us-east-1:895533407840:function:lambda_rds_to_kafka','us-east-1'),
									CONCAT('{"id_comment": NEW.id,"content":NEW.content, "created_at": "', TO_CHAR(NOW()::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')'"}')::json,
									'Event'
									);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
### Create Triger
"""
CREATE TRIGGER new_comment_trigger
  AFTER INSERT ON comments 
  FOR EACH ROW
  EXECUTE PROCEDURE call_lambda();
"""
## ----------------------- By el amigos ------------------------------- 

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
#         cursor = conn.cursor()
#         cursor.execute('INSERT INTO movies (id, title, overview, poster, year) VALUES (%s, %s, %s, %s, %s)',
#                        (row[0], row[1], row[2], row[3], row[4]))
#         cursor.close()
#         print(f'Inserted {row[1]}')

# sentiments = ['POSITIVE', 'NEGATIVE', 'NEUTRAL']
# for sentiment in sentiments:
#     cursor = conn.cursor()
#     cursor.execute('INSERT INTO sentiments (sentiment) VALUES (%s)', (sentiment,))
#     cursor.close()
#     print(f'Inserted {sentiment} into sentiments table')



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
