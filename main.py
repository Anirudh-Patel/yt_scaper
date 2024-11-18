import json
import os
import pickle
import re
import logging
from logging import exception
from re import search
import time

#from torch.utils.tensorboard.summary import video
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
#from pytube import YouTube
from pytubefix import YouTube
from pydub import AudioSegment
import yt_dlp
import speech_recognition as sr
from pymongo import MongoClient
from bson.objectid import ObjectId
import isodate
from collections import defaultdict
from google.cloud import language_v2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
from pymongo import MongoClient
from google.cloud import language_v1
from nltk.tokenize import sent_tokenize
import nltk
from tenacity import retry, wait_exponential, stop_after_attempt
from functools import wraps
from deepmultilingualpunctuation import PunctuationModel
import time
import datetime
from datetime import datetime


# Load environment variables from .env file
load_dotenv()


API_KEY = os.getenv('API_KEY')

connection_string = os.getenv("ATLAS_URI")
db_name = os.getenv("DB_NAME")


client = MongoClient(connection_string)
db = client[db_name]

#nltk.download('punkt')
#nltk.download('punkt_tab')

# test
# try:
#     client.admin.command('ping')
#     print("success")
# except Exception as e:
#     print(e)



def ensure_nltk_data():
    try:

        nltk.data.find('tokenizers/punkt')
    except LookupError:
        print("Downloading required NLTK data...")
        nltk.download('punkt', quiet=True)

SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']


def get_authenticated_service():
    api_key = os.environ["API_KEY"]

    return build('youtube', 'v3', developerKey=api_key)



def rate_limit_decorator(max_calls, period):
    def decorator(func):
        last_reset = time.time()
        calls = 0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_reset, calls
            current_time = time.time()

            if current_time - last_reset >= period:
                calls = 0
                last_reset = current_time

            if calls >= max_calls:
                sleep_time = period - (current_time - last_reset)
                time.sleep(max(0,sleep_time))
                calls = 0
                last_reset = time.time()

            calls += 1
            return func(*args, **kwargs)
        return wrapper
    return decorator

class APIerror(Exception):
    def __init__(self, message, error_code=None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

def safe_api_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"API call failed: {func.__name__}. Error: {str(e)}")
            raise APIerror(f"Failed to excecute {func.__name__}. Error: {str(e)}")
    return wrapper


@rate_limit_decorator(max_calls=100, period=100)
@safe_api_call
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


@rate_limit_decorator(max_calls=100, period=100)
@safe_api_call
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
            logging.error("no transcript from api")
            return None
    except TranscriptsDisabled:
        logging.error(f"Transcripts are disabled for video {video_id}.")
        return None
    except Exception as e:
        logging.error(f"Error fetching transcript for video {video_id}: {str(e)}")
        return None




@rate_limit_decorator(max_calls=100, period=100)
@safe_api_call
def get_channel_videos(service, channel_id):

    request = service.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    #print (f"JSON: \n{json.dumps(response,indent=2)}")
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
        #print(f"JSON: \n{json.dumps(response, indent=2)}")

        for i in response['items']:
            video_ids.append(i['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    #print(video_ids)
    videos = []
    for video_id in video_ids:
        video_request = service.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        video_response = video_request.execute()
        for v in video_response.get('items', []):

            duration = get_duration(v['contentDetails']['duration'])
            if duration >= 60:
                video_details = {
                    'video_id': v['id'],
                    'title': v['snippet']['title'],
                    'views': int(v['statistics']['viewCount']),
                    'transcript': get_video_transcript(v['id'])
                }
                videos.append(video_details)
                #print(f"got video details for {video_details}")
            else:
                print(f"Video {v['id']} is too short, skipping.")

    videos.sort(key=lambda x: x['views'], reverse=True)
    #print(f"after sorted list {videos}")

    return videos

@rate_limit_decorator(max_calls=100, period=100)
@safe_api_call


def split_text_into_chunks(text, chunk_size=400):

    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0

    for word in words:

        word_length = len(word) + 1


        if current_length + word_length > chunk_size:
            if current_chunk:
                chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_length = word_length
        else:
            current_chunk.append(word)
            current_length += word_length


    if current_chunk:
        chunks.append(' '.join(current_chunk))

    return chunks

def analyze_sentiment(text):

    try:

        print(f"Attempting sentiment analysis at {datetime.now}")

        client = language_v2.LanguageServiceClient()

        chunks = split_text_into_chunks(text)
        total_score, total_magnitude, count = 0, 0, 0

        for chunk in chunks:
            document = language_v2.Document(
                content=chunk,
                type_=language_v2.Document.Type.PLAIN_TEXT
            )

            response = client.analyze_sentiment(
                request={"document": document}
            )
            sentiment = response.document_sentiment
            total_score += sentiment.score
            total_magnitude += sentiment.magnitude
            count += 1
        if count > 0:
            average_score = total_score / count
            average_magnitude = total_magnitude / count
        else:
            average_score = 0
            average_magnitude = 0

        return average_score, average_magnitude

    except Exception as e:
        logging.error(f"Error in sentiment analysis: {str(e)}")
        return 0, 0


def get_duration(duration):
    return isodate.parse_duration(duration).total_seconds()

# def calculate_engagement_score(video_stats):
#     try:
#         views = int(video_stats['views'])
#         likes = int(video_stats['likes'])
#         dislikes = int(video_stats['dislikes'])
#         comment_count = int(video_stats['comment_count'])
#         watch_time
#
#
#         if views > 0:
#             like_rate =
def create_overview(videos, channel_id, channel_name):

    total_views = sum(video['views'] for video in videos)

    total_sentiment = sum(video['sentiment_score'] for video in videos)
    print(total_sentiment)
    total_magnitude = sum(video['sentiment_magnitude'] for video in videos)
    video_count = len(videos)

    avg_sentiment = total_sentiment / video_count if video_count > 0 else 0
    avg_magnitude = total_magnitude / video_count if video_count > 0 else 0

    overview_doc = {
        '_id': 'channel_overview',
        'document_type': 'overview',
        'channel_name': channel_name,
        'channel_id': channel_id,
        'total_views': total_views,
        'video_count': video_count,
        'average_sentiment': avg_sentiment,
        'average_magnitude': avg_magnitude,
        'last_updated': datetime.now(),

    }

    return overview_doc

@rate_limit_decorator(max_calls=30, period=60)
@safe_api_call
def store_videos(videos, db, channel_id, channel_handle):


    collection_name = f'videos_{channel_handle}'
    collection = db[collection_name]

    if collection_name in db.list_collection_names():
        print(f"Collection '{collection_name}' already exists. Updating documents...")
    else:
        print(f"Creating new collection '{collection_name}'.")


    try:
        collection.delete_one({'_id': 'channel_overview'})
    except Exception as e:
        logging.error(f"Error deleting existing overview: {str(e)}")


    processed_videos = []
    transcript_stats = defaultdict(int)
    total_score = 0
    total_magnitude = 0
    count = 0
    print(f"Total videos to process: {len(videos)}")




    for v in videos:
        print(f"Processing video: {v['video_id']}")
        sentiment_score, sentiment_magnitude = analyze_sentiment(v['transcript'])
        video_doc = {
            'video_id': v['video_id'],
            'title': v['title'],
            'views': v['views'],
            'transcript': v['transcript'],
            'sentiment_score': sentiment_score,
            'sentiment_magnitude': sentiment_score,
        }
        processed_videos.append(video_doc)
        try:
            collection.update_one(
                {'video_id': v['video_id']},
                {'$set': video_doc},
                upsert=True
            )
            transcript_stats['api_success'] += 1
            total_score += sentiment_score
            total_magnitude += sentiment_magnitude
            count += 1

        except Exception as e:
            logging.error(f"error processing video {v['video_id']}: {str(e)}")
            transcript_stats['api_failure'] += 1

        if count > 0:
            average_score = total_score / count
            average_magnitude = total_magnitude / count
        else:
            average_score = 0
            average_magnitude = 0

    overview = create_overview(processed_videos, channel_id, channel_handle)



    try:

        collection.update_one(
            {'_id': 'channel_overview'},
            {'$set': overview},
            upsert=True
        )


    except Exception as e:
        logging.error(f"error in storing videos: {str(e)}")
        return None

    return transcript_stats, average_score, average_magnitude










def main():
    youtube_service = get_authenticated_service()





    query = input("Enter YT channel name > ")
    channel_id = get_channel_id(youtube_service, query)
    channel_handle = query.strip('@').lower().replace(' ', '_')
    #ensure_nltk_data()

    if channel_id:
        videos = get_channel_videos(youtube_service, channel_id)

        x = store_videos(videos, db, channel_id, channel_handle)

        transcript_stats = x[0]
        average_score = x[1]
        average_magnitude = x[2]


        print("\nSummary:")
        print(f"Total videos attempted: {len(videos)}")
        print(f"Videos retrieved via API: {transcript_stats['api_success']}")
        print(f"Videos retrieved via transcription: {transcript_stats['transcription_success']}")
        print(f"API failures: {transcript_stats['api_failure']}")
        print(f"Transcription failures: {transcript_stats['transcription_failure']}")
        print(f"Average Sentiment Score: {average_score}")
        print(f"Average Sentiment Magnitude: {average_magnitude}")


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\nExecution time: {(end_time - start_time):.2f} seconds")


if __name__ == '__main__':
    main()
