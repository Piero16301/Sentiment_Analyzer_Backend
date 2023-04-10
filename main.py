import os

from imdb import Cinemagoer
import csv

# Create a new instance of the Cinemagoer class
ia = Cinemagoer()

# Get the top 250 movies
top250 = ia.get_top250_movies()


def full_size_cover(url):
    base, ext = os.path.splitext(url)
    i = url.count('@')
    s2 = url.split('@')[0]
    url = s2 + '@' * i + ext
    return url


# Get the full information and full size poster for movies
for movie in top250:
    ia.update(movie, ['main'])
    print('Added full information for {}'.format(movie['title']))

# Export the top 250 movies to a CSV file
header = ['id', 'title', 'overview', 'poster', 'year']
with open('top250.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    for movie in top250:
        cover = full_size_cover(movie['cover url'])
        writer.writerow([movie.movieID, movie['title'], movie['plot outline'], cover, movie['year']])
    print('Exported top 250 movies')

# Add reviews to the movies
for movie in top250:
    ia.update(movie, ['reviews'])
    print('Added {} reviews for {}'.format(len(movie['reviews']), movie['title']))

# Export reviews to a CSV file
header = ['title', 'content', 'date']
for movie in top250:
    with open('data/{}.csv'.format(movie.movieID), 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for review in movie['reviews']:
            # remove \ and , from the data
            title = review['title'].replace(',', '').replace('\\', '')
            content = review['content'].replace(',', '').replace('\\', '')
            date = review['date'].replace(',', '').replace('\\', '')
            writer.writerow([title, content, date])
    print('Exported {} reviews for {}'.format(len(movie['reviews']), movie['title']))
