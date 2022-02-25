from coveopush import CoveoPush
from coveopush import Document
import json
import urllib.request
import pickle

ORG_ID = "yr_org_id"
SRC_ID = "yr_src_id"
API_KEY = "yr_api_key"


def get_movies_and_directors():
    movies_list = []
    directors_list = []
    for i in range(50):
        with urllib.request.urlopen(
                f"https://coveo-movie-listing.herokuapp.com/movies?page={i + 1}") as response:
            movies_list += json.loads(response.read())
    for i in range(25):
        with urllib.request.urlopen(
                f"https://coveo-movie-listing.herokuapp.com/directors?page={i + 1}") as response:
            directors_list += json.loads(response.read())
    return movies_list, directors_list


def create_director_doc_per_movie(details):
    docs = []
    for curr_movie in details['movies']:
        curr_id = f"file://directors/{details['id']}/{curr_movie['id']}"
        fold_curr_id = f"directors{details['id']}{curr_movie['id']}"
        fold_parent_id = f"movies{curr_movie['id']}"
        mydoc = Document(curr_id)
        mydoc.Title = details["title"]
        mydoc.SetData(json.dumps(details))
        for k, v in details.items():
            if k in ['movies', 'title']:
                continue
            mydoc.AddMetadata(k, v)
        mydoc.AddMetadata('objecttype', 'director')
        mydoc.AddMetadata('foldingparent', fold_parent_id)
        mydoc.AddMetadata('foldingchild', fold_curr_id)
        mydoc.AddMetadata('foldingcollection', fold_parent_id)
        mydoc.FileExtension = ".json"
        docs.append(mydoc)
    return docs


def create_movie_doc(details):
    curr_id = f"file://movies/{details['id']}"
    mydoc = Document(curr_id)
    mydoc.Title = details["title"]
    mydoc.SetData(json.dumps(details))
    mydoc.FileExtension = ".json"
    for k, v in details.items():
        if k in ['directors', 'title']:
            continue
        new_key = k if k != 'year' else 'yearreleased'
        mydoc.AddMetadata(new_key, v)
    dir_names = []
    for curr_dir in details['directors']:
        dir_names.append(curr_dir['name'])
    mydoc.AddMetadata('directors', dir_names)
    fold_curr_id = f"movies{details['id']}"
    mydoc.AddMetadata('foldingparent', fold_curr_id)
    mydoc.AddMetadata('foldingchild', fold_curr_id)
    mydoc.AddMetadata('foldingcollection', fold_curr_id)
    mydoc.AddMetadata('objecttype', "movie")
    return mydoc


movies, directors = get_movies_and_directors()

# save/pickle movies and directors so you don't have to scrape every time
# moviefile = open('movies.pickle', 'ab')
# directorfile = open('directors.pickle', 'ab')
# pickle.dump(movies, moviefile)
# pickle.dump(directors, directorfile)

# load pickles
# moviefile = open('movies.pickle', 'rb')
# directorfile = open('directors.pickle', 'rb')
# movies = pickle.load(moviefile)
# directors = pickle.load(directorfile)

print("got all movies/directors, starting push...")


push = CoveoPush.Push(SRC_ID, ORG_ID, API_KEY)

push.Start(p_DeleteOlder=True)
push.SetSizeMaxRequest(150*1024*1024)
for movie in movies:
    push.Add(create_movie_doc(movie))
for director in directors:
    for doc in create_director_doc_per_movie(director):
        push.Add(doc)
push.End()

print("finished push")
