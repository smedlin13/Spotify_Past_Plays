import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

# def check_if_valid_data(df: pd.DataFrame) -> bool:
#     # Check if dataframe is empty
#     if df.empty:
#         print("No songs downloaded. Finishing execution")
#         return False 

#     # Primary Key Check
#     if pd.Series(df['played_at']).is_unique:
#         pass
#     else:
#         raise Exception("Primary Key check is violated")

#     # Check for nulls
#     if df.isnull().values.any():
#         raise Exception("Null values found")

#     # Check that all timestamps are of yesterday's date
#     yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
#     yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

#     timestamps = df["timestamp"].tolist()
#     for timestamp in timestamps:
#         if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
#             raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

#     return True

DATABASE_LOCATION = "sqlite:///Spotify_Past_Plays.sqlite"
USER_ID = "smedlin"
TOKEN = "BQDVxwr6_52kq_FaU5YcxUNQqISSVHHQ8OA7ds4ZpmBho-GfVZcnyMgokcBWEO6aUkvLkvoV6Ux_GycN3QLT-mUsaskwU1tSF80VEKw-hv5pPxPfvFOzEA7Dn15CBfC8N5fI_sp9i63kOCX4zXh6"

if __name__ == "__main__":

# payload = {"query":"query GamePage_Game($name: String!, $type: DirectoryType!, $limit: Int, $languages: [String!], $cursor: Cursor, $filters: StreamMetadataFilterInput) {\n  directory(name: $name, type: $type) {\n    id\n    displayName\n    ... on Community {\n      id\n      streams(first: $limit, after: $cursor, languages: $languages) {\n        edges {\n          cursor\n          node {\n            id\n            title\n            viewersCount\n            previewImageURL(width: 320, height: 180)\n            broadcaster {\n              id\n              login\n              displayName\n              __typename\n            }\n            game {\n              id\n              boxArtURL(width: 285, height: 380)\n              name\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        pageInfo {\n          hasNextPage\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ... on Game {\n      id\n      product {\n        id\n        __typename\n      }\n      streams(first: $limit, after: $cursor, languages: $languages, filters: $filters) {\n        edges {\n          cursor\n          node {\n            id\n            title\n            viewersCount\n            previewImageURL(width: 320, height: 180)\n            broadcaster {\n              id\n              login\n              displayName\n              __typename\n            }\n            game {\n              id\n              boxArtURL(width: 285, height: 380)\n              name\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        pageInfo {\n          hasNextPage\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n","variables":{"name":"PLAYERUNKNOWN'S BATTLEGROUNDS","limit":30,"languages":[],"type":"GAME","filters":{"hearthstoneBroadcasterHeroName":"","hearthstoneBroadcasterHeroClass":"","hearthstoneGameMode":"","overwatchBroadcasterCharacter":"","leagueOfLegendsChampionID":"","counterStrikeMap":"","counterStrikeSkill":""}},"operationName":"GamePage_Game"}
# r = requests.post('https://gql.twitch.tv/gql', data=payload)

  headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",  
    "Authorization" : "Bearer {token}".format(token=TOKEN)
  }

  today = datetime.datetime.now()
  yesterday = today - datetime.timedelta(days=1)
  yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

  r = requests.get("https://api.spotify.com/v1/me/player/recently-played?afterw={time}".format(time=yesterday_unix_timestamp), headers = headers)

  data = r.json()


  song_names = []
  artist_names = []
  played_at_list = []
  timestamps = []

    # Extracting only the relevant bits of data from the son object      
  for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

  # Prepare a dictionary in order to turn it into a pandas dataframe below       
  song_dict = {
      "song_name" : song_names,
      "artist_name": artist_names,
      "played_at" : played_at_list,
      "timestamp" : timestamps
  }

  song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    
  print(song_df)
  # Validate
  # if check_if_valid_data(song_df):
  #     print("Data valid, proceed to Load stage")


# Load

  # engine = sqlalchemy.create_engine(DATABASE_LOCATION)
  # conn = sqlite3.connect('my_played_tracks.sqlite')
  # cursor = conn.cursor()

  # sql_query = """
  #     CREATE TABLE IF NOT EXISTS my_played_tracks(
  #         song_name VARCHAR(200),
  #         artist_name VARCHAR(200),
  #         played_at VARCHAR(200),
  #         timestamp VARCHAR(200),
  #         CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
  #     )
  #     """

  # cursor.execute(sql_query)
  # print("Opened database successfully")
  # try:
  #     song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
  # except:
  #     print("Data already exists in the database")

  # conn.close()
  # print("Close database successfully")