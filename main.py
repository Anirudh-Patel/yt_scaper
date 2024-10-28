import json
import os
import pickle
import re
from re import search

from pytube.extract import video_id
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from pytube import YouTube
from pydub import AudioSegment
import speech_recognition as sr
from pymongo import MongoClient
from bson.objectid import ObjectId
import isodate


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()

# API key stored in secrets in repl
API_KEY = os.getenv('API_KEY')

connection_string = os.getenv("ATLAS_URI")
db_name = os.getenv("DB_NAME")


client = MongoClient(connection_string)
db = client[db_name]

# test
# try:
#     client.admin.command('ping')
#     print("success")
# except Exception as e:
#     print(e)

# collection_name = 'Videos'
# collection = db[collection_name]

#test collection

# test_doc = {
#     "video_id": "asdhjaf",
#     "transcript": "a;lksjh",
#     "language": "en"
# }
# result = collection.insert_one(test_doc)
# print(f"doc ID: {result.inserted_id}")
#
# document_id = ObjectId("6712237e7c614ac5cb0701d7")
# update_result = collection.update_one(
#                                       {'_id': document_id},
#                                     {'$set': {'transcript': 'Updated trasncript'}})
# if update_result.modified_count > 0:
#     print("success ")
# else:
#     print("FAIL")


SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']


def get_authenticated_service():
    api_key = os.environ["API_KEY"]

    return build('youtube', 'v3', developerKey=api_key)


# def search_videos(service, query):
#     request = service.search().list(
#         part="snippet",
#         maxResults=3,
#         q=query
#     )
#     response = request.execute()
#
#     video_ids = []
#     for item in response['items']:
#         if item['id']['kind'] == 'youtube#video':
#             video_ids.append(item['id']['videoId'])
#
#     return video_ids


def get_channel_id(service, handle):

    request = service.channels().list(
        part="id",
        forUsername=handle
    )
    response = request.execute()

    if 'items' in response and response['items']:
        return response['items'][0]['id']

    search_request = service.search().list(
        part="snippet",
        q=handle,
        type="channel",
        maxResults=1
    )
    search_response = search_request.execute()

    if 'items' in search_response and search_response['items']:
        return search_response['items'][0]['id']['channelId']

    print("FAIL")
    return None


def get_video_transcript(video_id, language='en', filter_fillers=None):
    try:

        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[language])
        filler_words = ['um', 'uh', 'ah']

        if filter_fillers is None:
            filter_fillers = True

        if filter_fillers:

            filtered_transcript = ' '.join([
                segment['text']
                for segment in transcript
                if not any(word in segment['text'].lower()
                           for word in filler_words)])

            return filtered_transcript
        else:
            return ' '.join([segment['text'] for segment in transcript])
    except NoTranscriptFound:
            print("no transcript from api")
            return transcribe_video(video_id)
    except TranscriptsDisabled:
        print(f"Transcripts are disabled for video {video_id}.")
        return transcribe_video(video_id)
    except Exception as e:
        print(f"Error fetching transcript for video {video_id}: {str(e)}")
        return None


def transcribe_video(video_id):


    try:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        yt = YouTube(video_url)
        audio_stream = yt.streams.filter(only_audio=True).first()
        audio_file = audio_stream.download(filename='audio.mp4')

        audio = AudioSegment.from_file(audio_file)
        audio.export("audio.wav", format="wav")

        recognizer = sr.Recognizer()

        with sr.AudioFile("audio.wav") as source:
            audio_data = recognizer.record(source)
            transcript = recognizer.recognize_google_cloud(audio_data)

        return transcript

    except Exception as e:
        print(f"error with video:{video_id}: {str(e)}")



def get_channel_videos(service, channel_id):

    request = service.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()

    if 'items' in response and response['items']:
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    else:
        print("No items found in the response. Please check the channel ID or URL.")
        return []

    video_ids = []
    next_page_token = None

    while True:
        request = service.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for i in response['items']:
            video_ids.append(i['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break


    videos = []
    for video_id in video_ids:
        video_request = service.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        video_response = video_request.execute()
        for v in video_response['items']:

            duration = get_duration(v['contentDetails']['duration'])
            if duration >= 60:
                video_details = {
                    'video_id': v['id'],
                    'title': v['snippet']['title'],
                    'views': int(v['statistics']['viewCount']),
                    'transcript': get_video_transcript(v['id'])
                }
                videos.append(video_details)
            else:
                print("short")

    videos.sort(key=lambda x: x['views'], reverse=True)

    return videos




def get_duration(duration):
    return isodate.parse_duration(duration).total_seconds()

def store_videos(videos, db, channel_id):

    collection_name = f'channel_{channel_id}'
    collection = db[collection_name]

    if collection_name in db.list_collection_names():
        print(f"Collection '{collection_name}' already exists. Updating documents...")
    else:
        print(f"Creating new collection '{collection_name}'.")


    for v in videos:
        video_doc = {
            'video_id': v['video_id'],
            'title': v['title'],
            'views': v['views'],
            'transcript': v['transcript']
        }

        collection.update_one(
            {'video_id': v['video_id']},
            {'$set': video_doc},
            upsert=True)

        print(f"Added video with id: {v['video_id']}" )




# StudioMcGee

#test document
def insert_video_info(video_id, title, views, transcript):

    collection_name = 'videos_test'
    collection = db[collection_name]


    video_document = {
        'video_id': video_id,
        'title': title,
        'views': views,
        'transcript': transcript
    }


    result = collection.insert_one(video_document)
    print(f"Inserted video with ID: {result.inserted_id}")

#




def main():
    youtube_service = get_authenticated_service()


    #query = input("Enter keywords to search for videos: ")
    #video_ids = search_videos(youtube_service, query)

    query = input("Enter YT channel URL")
    channel_id = get_channel_id(youtube_service, query)

    if channel_id:
        videos = get_channel_videos(youtube_service, channel_id)
        store_videos(videos, db, channel_id)




    #test insert
    video_id = "example_video_id"
    title = "Example Video Title"
    views = 12345
    transcript = "This is an example transcript."

    # Insert the example video information into the database
    insert_video_info(video_id, title, views, transcript)

'''
for video_id in video_ids:
    transcript_type = 
    input("Do you want a full transcript (F) or filtered transcript (E)? ").lower()
    edited = transcript_type == 'e'
    language = input("Enter the language code (e.g., 'en' for English): ").lower()
    transcript = get_video_transcript(video_id, language, edited)

    if transcript:
        print(f"\n{'Filtered' if edited else 'Full'} transcript for video ID: {video_id}")
        print(transcript)
        print("\n")
    '''
# FEAUTRES TO BE ADDED
# 1. filter out filler words
# 2. filter out videos without subtitles
# 3  Sort by the data


if __name__ == '__main__':
    main()
