from sqlalchemy.orm import Session

from sql_app import models

from . import schemas
from sqlalchemy import and_, or_, func, cast, Integer
from typing import Union
import json

# check if there are duplicate movies
def get_movie_match(db: Session, openDate: str, title: str, runningTimeMinute: str, titleEng: str):
    name = db.query(models.Movie).filter(models.Movie.title == title).first()
    nameEng = db.query(models.Movie).filter(models.Movie.titleEng == titleEng).first()
    runningtime = db.query(models.Movie).filter(models.Movie.runningTimeMinute == runningTimeMinute).first()
    openDate = db.query(models.Movie).filter(models.Movie.openDate == openDate).first()
    result = 0
    if nameEng is not None and name is not None and runningtime is not None and openDate is not None:
        result = 1
    return result

# insert movie metadata into SQLite database
def insert_data_into_db(db: Session, data: schemas.Movie):
    db_movie = models.Movie(title = data.title,
                            titleEng = data.titleEng,
                            openDate = data.openDate,
                            runningTimeMinute = data.runningTimeMinute)
    db_movie.set_list_field("genre", data.genre)
    db_movie.set_list_field("actors", data.actors)
    db_movie.set_list_field("directors", data.directors)
    db_movie.set_list_field("producer", data.producer)
    db_movie.set_list_field("distributor", data.distributor)
    db_movie.set_list_field("keywords", data.keywords)
    db_movie.set_list_field("posterUrl", data.posterUrl)
    db_movie.set_list_field("vodUrl", data.vodUrl)
    db_movie.set_dict_field("synopsis", data.synopsis)

    db.add(db_movie)
    db.commit()
    db.refresh(db_movie)
    return db_movie

# search movie : accessing from the SQLite database. 
def search_movies(db: Session, search_query: Union[str, None] = None):
    query = db.query(models.Movie)
    if search_query is not None:
        matching_movies = []
        for movie in query.all():
            movie_directors = json.loads(movie.directors)
            movie_actors = json.loads(movie.actors)
            movie_keywords = json.loads(movie.keywords)
            if search_query in movie_directors:
                matching_movies.append(movie)
            if search_query in movie_actors:
                matching_movies.append(movie)
            if search_query in movie_keywords:
                matching_movies.append(movie)
            if movie.title == search_query:
                matching_movies.append(movie)
        return matching_movies

    return query.all()

# filtering tool: filter by the range of year released (openDate)
def get_opendate(db: Session, openyear: Union[int, None]=0, endyear: Union[int, None]=9999):
    query = db.query(models.Movie).filter(models.Movie.openDate != None)

    if openyear is not None:
        query = query.filter(func.cast(func.substring(models.Movie.openDate, 1, 4), Integer) >= openyear)
    if endyear is not None:
        query = query.filter(func.cast(func.substring(models.Movie.openDate, 1, 4), Integer) <= endyear)

    return query.all()

# filtering tool: filter by genres (union method)
def get_genre(db: Session, genres: list[str]):
    query = db.query(models.Movie).filter(models.Movie.genre != None)

    if genres is not None and genres:
        matching_movies = []
        for movie in query.all():
            movie_genres = json.loads(movie.genre)
            if any(genre in movie_genres for genre in genres):
                matching_movies.append(movie)
        return matching_movies

    return query.all()

# Delete all from the databse
def delete_all_records(db: Session):
    db.query(models.Movie).delete()
    db.commit()