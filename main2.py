from typing import Union
from fastapi import FastAPI, Query, Depends, FastAPI, HTTPException
import requests
import json
from sqlalchemy.orm import Session
from sql_app import crud, models, schemas
from sql_app.database import SessionLocal, engine
import pandas as pd
import datetime
import ast
from fastapi.middleware.cors import CORSMiddleware
from enum import Enum

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Configure CORS using middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://smdb-front.vercel.app", "http://localhost:3000"],  # Use ["*"] to allow all origins or specify your allowed domains
    allow_methods=["*"],  
    allow_headers=["*"],  
    allow_credentials=True, 
)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"

def all_movies(dataset_list: list, result: list):
    for movie in dataset_list:
        if movie['type'] =='dataverse':
            continue
        result.append(json.loads(movie['description']))
    return result

# Search bar tool: search movie; access directly from Dataverse database
# returns a list with each movie metadata as an item in dict format
# @app.get("/movies/") 
# def read_movie(q: Union[str, None] = None):
#     result = []
#     condition = True
#     start = 0
#     rows = 10
#     while(condition):
#         if q == None:
#             url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&start={start}"
#         else:
#             url = f"https://snu.dataverse.ac.kr/api/search?q={q}&subtree=movies&start={start}"
#         headers = {
#             "X-Dataverse-key": API_KEY
#         }
#         response = requests.get(url, headers = headers)
#         total = response.json()["data"]["total_count"]
#         start = start + rows
#         condition = start < total

#         if response.status_code == 200:
#             dataset_list = response.json()["data"]["items"]
#             result = all_movies(dataset_list, result)
#         else:
#             return "검색 결과 없음"
#     return result
    
    
# @app.get("/movies/search/")
# def search_movies_endpoint(search_query: Union[str, None] = None, db: Session = Depends(get_db)):
#     movies = crud.search_movies(db, search_query)
#     return movies

# @app.post("/movies/upload/")
# def create_movies(data: list[schemas.Movie], db: Session = Depends(get_db)):
#     results = []
#     for per_movie in data:
#         print(per_movie)
#         db_movie = crud.get_movie_match(db, openDate=per_movie.openDate, title=per_movie.title, titleEng=per_movie.titleEng, runningTimeMinute=per_movie.runningTimeMinute)
#         if db_movie:
#             raise HTTPException(status_code=400, detail="Movie already registered")
#         result = crud.insert_data_into_db(db=db, data=per_movie)
#         results.append(result)
#     return results

# @app.get("/movies/filter_by_opendate/")
# def filter_movies_by_opendate(openyear: Union[int, None] = None, endyear: Union[int, None] = None, db: Session = Depends(get_db)):
#     movies = crud.get_opendate(db, openyear, endyear)
#     return movies

# @app.get("/movies/filter_by_genre/")
# def filter_movies_by_genre(genres: list[str] = Query(None, description="List of genres to filter by"), db: Session = Depends(get_db)):
#     movies = crud.get_genre(db, genres)
#     return movies

class Genre(str, Enum):
    action = "액션"
    drama = "드라마"
    comedy = "코미디"
    thriller = "스릴러"
    fantasy = "SF/판타지"
    romance = "로맨스"
    adventure = "어드벤처"
    horror = "공포"
    crime = "범죄"
    animation = "애니메이션"

@app.get("/movies/filter/")
def filter(openyear: Union[int, None] = None, endyear: Union[int, None] = None, genres: list[Genre] = Query(None, description="List of genres to filter by"), 
           q: Union[str, None] = None, db: Session = Depends(get_db)):
    movies = crud.searchquery(db, genres, openyear, endyear, q)
    return movies

# Most Loved Movies
@app.get("/movies/mostloved/")
def mostloved(db: Session = Depends(get_db)):
    result = []
    condition = True
    start = 0
    rows = 10
    while(condition):
        url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=mostloved&start={start}"
        headers = {
            "X-Dataverse-key": API_KEY
        }
        response = requests.get(url, headers = headers)
        total = response.json()["data"]["total_count"]
        start = start + rows
        condition = start < total

        if response.status_code == 200:
            dataset_list = response.json()["data"]["items"]
            result = all_movies(dataset_list, result)
        else:
            return "검색 결과 없음"
    result.append(len(result))
    return result

# @app.post("/delete_all_records/")
# def delete_records(db: Session = Depends(get_db)):
#     crud.delete_all_records(db)
#     return {"message": "All records deleted"}


def today():
    file_path = "./kobis 8_21.csv"
    daily_boxoffice =  pd.read_csv(file_path, skiprows=6)
    daily_boxoffice.reset_index(drop=True, inplace=True) # 제거한 행에 대한 인덱스 재설정(열이름이 지저분하지만 무시하자)
    today_list = daily_boxoffice.iloc[:,1].tolist() # today_list: 일별 박스오피스 영화 list
    today_list = pd.Series(today_list).dropna().tolist()
    return today_list

@app.get("/movies/onscreen")
def onscreen():
    # 1) 상영중 영화 리스트
    today_list = today()
    # 2) 상영중 영화 리스트에 대한 반복문
    onscreen_list = []
    for movie in today_list:
        # movies dataverse에서 영화명 검색
        url = f"https://snu.dataverse.ac.kr/api/search?q={movie}&subtree=movies&"
        headers = {
            "X-Dataverse-key": API_KEY
        }
        response = requests.get(url, headers = headers)

        if response.status_code == 200:
            result = response.json()["data"]["items"]
            if not result:  # 검색 결과가 없는 경우
                print(f"[Warning] {movie}에 대한 검색 결과가 없습니다.")
            else:  # 데이터셋명이 영화명과 동일한 경우 해당 영화의 description을 onscreen_list에 넣기
                for item in result:
                    if item['name'] == movie:
                        onscreen_list.append(ast.literal_eval(item['description']))
    return onscreen_list

@app.get("/movies/comingsoon")
def comingsoon():
    comingsoon_list = []
    condition = True
    start = 0
    rows = 1000
    # 1000개씩 반복해서 정보 불러오기
    while(condition):
        url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&sort=name&order=asc&per_page=1000&start={start}"
        headers = {
                "X-Dataverse-key": "9ab68902-3f25-4848-8384-3a217a763e5a"
            }
        response = requests.get(url, headers = headers)
        
        # 1) 영화 1000개 단위로 불러오기
        total = response.json()["data"]["total_count"]
        start = start + rows
        condition = start < total

        # 2) 각 영화의 description 불러와서 개봉일과 오늘날짜 비교하기
        if response.status_code == 200:
            dataset_list = response.json()["data"]["items"]
            for movie in dataset_list:
                if movie.get("description"): # description 필드가 비어있지 않은 경우 출력
                    description = eval(movie['description']) # 문자열 description을 딕셔너리 형태로 변환
                    opendate_str = description["openDate"] # 문자열 opendate_str
                    if opendate_str != '': # opendate가 있을 경우, datetime 형태로 변환
                        opendate = datetime.datetime.strptime(opendate_str, "%Y.%m.%d").date()
                        if opendate > datetime.date.today(): # 개봉일 > 오늘 날짜일 경우 commingsoon_list에 해당 영화 description 추가
                            comingsoon_list.append(description)
                        else: continue
    return comingsoon_list

# 6. 상영 완료
@app.get("/movies/offscreen")
def offscreen():
    global today_list
    offscreen_list = []
    condition = True
    start = 0
    rows = 1000
    # 1000개씩 반복해서 정보 불러오기
    while(condition):
        url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&sort=name&order=asc&per_page=1000&start={start}"
        headers = {
                "X-Dataverse-key": "9ab68902-3f25-4848-8384-3a217a763e5a"
            }
        response = requests.get(url, headers = headers)
        
        # 1) 영화 1000개 단위로 불러오기
        total = response.json()["data"]["total_count"]
        start = start + rows
        condition = start < total

        # 2) 각 영화의 description 불러와서 개봉일과 오늘날짜 비교하기
        if response.status_code == 200:
            dataset_list = response.json()["data"]["items"]
            for movie in dataset_list:
                if movie.get("description"): # description 필드가 비어있지 않은 경우 출력
                    description = eval(movie['description']) # 문자열 description을 딕셔너리 형태로 변환
                    opendate_str = description["openDate"] # 문자열 opendate_str
                    if opendate_str != '': # opendate가 있을 경우, datetime 형태로 변환
                        opendate = datetime.datetime.strptime(opendate_str, "%Y.%m.%d").date()
                        if (opendate < datetime.date.today())&(movie not in today_list): # 개봉일 <  오늘 날짜일 경우 commingsoon_list에 해당 영화 description 추가
                            offscreen_list.append(description)
                        else: continue
                    else: continue
    return offscreen_list
