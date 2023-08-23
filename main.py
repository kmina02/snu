from fastapi import FastAPI, Depends, HTTPException, Header, Query
from typing import List, Optional, Union
from uuid import UUID
import json # To change "description" into dict
import requests
from datetime import date
import datetime
import pandas as pd
import ast
from sqlalchemy.orm import Session
from sql_app import crud, models, schemas
from sql_app.database import SessionLocal, engine

app = FastAPI()

# 0. Setting -------------------------------------------------------------------------------------------------------------------
# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# all_movies_dataset: return every description of all datasets
def all_movies_dataset(dataset_list: list, result: list):
    for movie in dataset_list:
        result.append(json.loads(movie["description"]))
    return result

# API_KEY
API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"

# 1. Search bar tool: search movie; access directly from Dataverse database -------------------------------------------------------------------------
# 1) returns a list with each movie metadata as an item in dict format
@app.get("/movies/") 
def read_movie(q: Union[str, None] = None):
    result = []
    condition = True
    start = 0
    rows = 10
    while(condition):
        if q == None:
            url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&start={start}"
        else:
            url = f"https://snu.dataverse.ac.kr/api/search?q={q}&subtree=movies&start={start}"
        headers = {
            "X-Dataverse-key": API_KEY
        }
        response = requests.get(url, headers = headers)
        total = response.json()["data"]["total_count"]
        start = start + rows
        condition = start < total

        if response.status_code == 200:
            dataset_list = response.json()["data"]["items"]
            result = all_movies_dataset(dataset_list, result)
        else:
            return "검색 결과 없음"
    return result

# 2) search from sql db
@app.get("/movies/search/")
def search_movies_endpoint(search_query: Union[str, None] = None, db: Session = Depends(get_db)):
    movies = crud.search_movies(db, search_query)
    return movies

# 3) save movie data in database
@app.post("/movies/upload/")
def create_movies(data: list[schemas.Movie], db: Session = Depends(get_db)):
    results = []
    for per_movie in data:
        print(per_movie)
        db_movie = crud.get_movie_match(db, openDate=per_movie.openDate, title=per_movie.title, titleEng=per_movie.titleEng, runningTimeMinute=per_movie.runningTimeMinute)
        if db_movie:
            raise HTTPException(status_code=400, detail="Movie already registered")
        result = crud.insert_data_into_db(db=db, data=per_movie)
        results.append(result)
    return results

# 4) filter by openyear
@app.get("/movies/filter_by_opendate/")
def filter_movies_by_opendate(openyear: Union[int, None] = None, endyear: Union[int, None] = None, db: Session = Depends(get_db)):
    movies = crud.get_opendate(db, openyear, endyear)
    return movies

# 5) filter by genre
@app.get("/movies/filter_by_genre/")
def filter_movies_by_genre(genres: list[str] = Query(None, description="List of genres to filter by"), db: Session = Depends(get_db)):
    movies = crud.get_genre(db, genres)
    return movies

# 6) delete all records in db
@app.post("/delete_all_records/")
def delete_records(db: Session = Depends(get_db)):
    crud.delete_all_records(db)
    return {"message": "All records deleted"}

# 1. movies dataverse 속 모든 dataset의 description 정보를 불러오기--------------------------------------------------------------------------------------------------
# HTTP 요청을 통해 정보 불러오기
# base_url = "http://snu.dataverse.ac.kr"
# database_name = "movies"
# api_endpoint = f"{base_url}/api/search?q=*&subtree={database_name}"
# API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"
# headers = {
#         "X-Dataverse-key": API_KEY
#     } 

# response = requests.get(api_endpoint, headers = headers)
# dataset_list = response.json()["data"]["items"] # 데이터베이스의 모든 데이터셋 조회

# # 모든 데이터셋의 description을 get 요청으로 불러오기
# for i in range(30):
#     dataset_id = dataset_list[i]["global_id"]  #doi를 dataset_id로 칭하기
#     @app.get(f"/dataset/{dataset_id}")
#     async def dataset_description(dataset_id = {dataset_id}):
#         dataset_detail_endpoint = f"{base_url}/api/datasets/:persistentId/?persistentId={dataset_id}"
#         detail_response = requests.get(dataset_detail_endpoint, headers = headers)
#         detail_data = detail_response.json()["data"]
#         description = detail_data["latestVersion"]["metadataBlocks"]["citation"]["fields"][3]["value"][0]["dsDescriptionValue"]["value"]
#         return json.loads(description) # description 정보를 dict 형태로 return하기

# 2. 연도 필터링 함수 ---------------------------------------------------------------------------------------------------------------------------------------------
# 일단 테스트로 db에 영화 10개 넣기


# @app.get(f"/filter/openyear={openyear}&endyear={endyear}")
# def get_opendate(db: Session, openyear: int, endyear: int):
#     return db.query(models.Movie).filter(
#         and_(
#             func.extract('year', func.cast(func.substring(models.Movie.openDate, 1, 4), int)) >= openyear,
#             func.extract('year', func.cast(func.substring(models.Movie.openDate, 1, 4), int)) <= endyear
#         )
#     )
# 3. 장르 필터링 함수 ----------------------------------------------------------------------------------------------------------------------------------------
# def get_genre(db: Session, genres: list):
#     return db.query(models.Movie).filter(models.Movie.genre.in_(genres))

# 4. 상영 중 ----------------------------------------------------------------------------------------------------------------------------------------

# 4. 상영 중
@app.get("/movies/today")
def today():
    file_path = "C:\projects\myapi\KOBIS_일별_박스오피스_2023-06-06.xlsx"
    daily_boxoffice =  pd.read_excel(file_path, skiprows=6)
    daily_boxoffice.reset_index(drop=True, inplace=True) # 제거한 행에 대한 인덱스 재설정(열이름이 지저분하지만 무시하자)
    today_list = daily_boxoffice.iloc[:,1].tolist() # today_list: 일별 박스오피스 영화 list
    today_list = pd.Series(today_list).dropna().tolist()
    return today_list

@app.get("/movies/onscreen")
async def onscreen():
    # 1) 상영중 영화 리스트
    global today_list
    # 2) 상영중 영화 리스트에 대한 반복문
    onscreen_list = []
    for movie in today_list:
        # movies dataverse에서 영화명 검색
        API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"
        url = f"https://snu.dataverse.ac.kr/api/search?q={movie}&subtree=movies&"
        headers = {
            "X-Dataverse-key": API_KEY
        }
        response = requests.get(url, headers = headers, timeout=5)

        if response.status_code == 200:
            result = response.json()["data"]["items"]
            if not result:  # 검색 결과가 없는 경우
                print(f"[Warning] {movie}에 대한 검색 결과가 없습니다.")
            else:  # 데이터셋명이 영화명과 동일한 경우 해당 영화의 description을 onscreen_list에 넣기
                for item in result:
                    if item['name'] == movie:
                        onscreen_list.append(ast.literal_eval(item['description']))
    return onscreen_list

# 5. 상영 예정
@app.get("/movies/comingsoon")
async def comingsoon():
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
                        if opendate > date.today(): # 개봉일 > 오늘 날짜일 경우 commingsoon_list에 해당 영화 description 추가
                            comingsoon_list.append(description)
                        else: continue
    return comingsoon_list

# 6. 상영 완료
@app.get("/movies/offscreen")
async def offscreen():
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
                        if (opendate < date.today())&(movie not in today_list): # 개봉일 <  오늘 날짜일 경우 commingsoon_list에 해당 영화 description 추가
                            offscreen_list.append(description)
                        else: continue
                    else: continue
    return offscreen_list

@app.get("/movieid/{movie_code}")
async def movieid(movie_code):
    # 1) movie_code를 통해 kobis Open API로부터 영화 정보 얻기
    # 영화 상세정보 요청 url
    url: str = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
    kobis_key = "8616582b33a50c5fa6b24e2155b5c1e3"

    # query parameter
    params: dict = {"key": kobis_key,
                    "movieCd": movie_code}

    # HTTP response
    response = requests.get(url, params=params)

    # JSON deserialize
    response_json = json.loads(response.text)
    result = response_json["movieInfoResult"]['movieInfo']

    # get running time and a movie name
    runtime_kobis = result["showTm"]
    moviename_kobis = result["movieNm"]

    # 2) Open API로부터 얻은 영화명 정보를 Movies Dataverse에 검색
    # search the movie name in dataverse
    API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"
    url = f"https://snu.dataverse.ac.kr/api/search?q={moviename_kobis}&subtree=movies&"
    headers = {
        "X-Dataverse-key": API_KEY
    }
    response = requests.get(url, headers = headers, timeout=5)

    # if there is no error, retrive search results
    if response.status_code == 200:
        result = response.json()["data"]["items"]
        if not result:  # 검색 결과가 없는 경우
            print(f"[Warning] {moviename_kobis}에 대한 검색 결과가 없습니다.")
        else:  # 데이터셋명이 영화명과 동일한 경우 해당 영화의 description을 출력
            for item in result:
                if item['description']["runningTimeMinute"] == runtime_kobis: # 러닝타임이 kobis Open API로부터 얻은 정보와 동일할 경우 영화 메타데이터 리턴
                    return item['description']


# # 모든 description 정보를 하나의 리스트로 만들기 -------------------------------------------------------------------------------------------------
# API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"

# def all_movies_dataset(dataset_list: list, result: list):
#     for movie in dataset_list:
#         result.append(json.loads(movie["description"]))
#     return result

# ## when the user types via search bar, returns a list with each movie data as an item in dict format
# @app.get("/movies/") 
# def read_movie():
#     result = []
#     condition = True
#     start = 0
#     rows = 1000
#     while(condition):
#         url = f"https://snu.dataverse.ac.kr/api/search?q=*&subtree=movies&per_page=1000&start={start}"
#         headers = {
#             "X-Dataverse-key": API_KEY
#         }
#         response = requests.get(url, headers = headers)
#         total = response.json()["data"]["total_count"]
#         start = start + rows
#         condition = start < total

#         if response.status_code == 200:
#             dataset_list = response.json()["data"]["items"]
#             result = all_movies_dataset(dataset_list, result)
#         else:
#             return "검색 결과 없음"

#     for item in result:
#         list_item = MovieList(title=item['title'], genre=item['genre'], synopsis = item['synopsis'], openDate = item['openDate'], runningTimeMinute = item['runningTimeMinute'])
#         session.add(list_item)

#     return result[:5]

    # id = Column(Integer, primary_key=True, index=True)
    # title = Column(String)
    # titleEng = Column(String)
    # genre = Column(String)
    # synopsis = Column(String)
    # openDate = Column(String)
    # runningTimeMinute = Column(String)
    # actors = Column(String)
    # directors = Column(String)
    # producer = Column(String)
    # distributor = Column(String)
    # keywords = Column(String)
    # posterUrl = Column(String)
    # vodUrl = Column(String)

# # results 리스트 속 모든 데이터셋의 description을 get 요청으로 불러오기
# base_url = "http://snu.dataverse.ac.kr"
# database_name = "movies"
# start = 0
# rows = 1000
# condition = True
# API_KEY = "9ab68902-3f25-4848-8384-3a217a763e5a"

# while(condition):
#     api_endpoint = f"{base_url}/api/search?q=*&subtree={database_name}&per_page=1000&start={start}"
#     headers = {
#         "X-Dataverse-key": API_KEY
#     }
#     response = requests.get(api_endpoint, headers = headers)
#     total = response.json()["data"]["total_count"]
#     start = start + rows
#     condition = start < total

#     if response.status_code == 200:
#         detail_data = detail_response.json()["data"]
#     else:
#         return "검색 결과 없음"


#     dataset_list = response.json()["data"]["items"] # 데이터베이스의 모든 데이터셋 조회

# for dataset in dataset_list:
#     dataset_id = dataset["global_id"]  #doi를 dataset_id로 칭하기
#     dataset_detail_endpoint = f"{base_url}/api/datasets/:persistentId/?persistentId={dataset_id}"
#     detail_response = requests.get(dataset_detail_endpoint)
#     detail_data = detail_response.json()["data"]
#     description = detail_data["latestVersion"]["metadataBlocks"]["citation"]["fields"][3]["value"][0]["dsDescriptionValue"]["value"]
#     movie_list.append(json.loads(description)) # 영화 정보가 담겨있는 리스트, movie_list 생성
#     @app.get(f"/dataset/{dataset_id}")
#     async def dataset_description(dataset_id = {dataset_id}):
#         return json.loads(description) # description 정보를 dict 형태로 return하기

# for movie in MovieList:
#     list_item = MovieList(title=movie['title'], genre=movie['genre'])
#     session.add(list_item)

# session.commit()


# # 내비게이션 바에서 [카테고리] 버튼을 클릭한 경우
# genres = ['드라마', '액션', '코미디', '스릴러', 'SF/판타지', '로맨스', '어드벤처', '공포', '애니메이션', '범죄']
# ## [전체]태그가 기본값
# # @app.get("/movies/allgenre")
# # async def get_movies_by_all_genre():
# #     return movie_list
# # # 장르별 
# # for genre in genres:
# #     @app.get(f"/movies/genre={genre}")
# #     async def get_movies_by_genre(genre = {genre}):
# #         filtered_movies = [movie for movie in movie_list if movie["genre"][0] == genre]
# #         return filtered_movies


# # # results 리스트 속 모든 데이터셋의 description을 get 요청으로 불러오기
# # for dataset in dataset_list:
# #     dataset_id = dataset["global_id"]  #doi를 dataset_id로 칭하기
# #     dataset_detail_endpoint = f"{base_url}/api/datasets/:persistentId/?persistentId={dataset_id}"
# #     detail_response = requests.get(dataset_detail_endpoint)
# #     detail_data = detail_response.json()["data"]
# #     description = detail_data["latestVersion"]["metadataBlocks"]["citation"]["fields"][3]["value"][0]["dsDescriptionValue"]["value"]
# #     movie_list.append(json.loads(description)) # 영화 정보가 담겨있는 리스트, movie_list 생성
# #     @app.get(f"/dataset/{dataset_id}")
# #     async def dataset_description(dataset_id = {dataset_id}):
# #         return json.loads(description) # description 정보를 dict 형태로 return하기

# # for movie in MovieList:
# #     list_item = MovieList(title=movie['title'], genre=movie['genre'])
# #     session.add(list_item)

# # session.commit()


# # 내비게이션 바에서 [카테고리] 버튼을 클릭한 경우
# genres = ['드라마', '액션', '코미디', '스릴러', 'SF/판타지', '로맨스', '어드벤처', '공포', '애니메이션', '범죄']
# ## [전체]태그가 기본값
# # @app.get("/movies/allgenre")
# # async def get_movies_by_all_genre():
# #     return movie_list
# # # 장르별 
# # for genre in genres:
# #     @app.get(f"/movies/genre={genre}")
# #     async def get_movies_by_genre(genre = {genre}):
# #         filtered_movies = [movie for movie in movie_list if movie["genre"][0] == genre]
# #         return filtered_movies
