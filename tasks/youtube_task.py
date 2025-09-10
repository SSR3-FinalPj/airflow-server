import logging
import json
import os
import pathlib
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
from pathlib import Path

# .env 파일에서 환경변수 로드
load_dotenv()

# --- 전역 설정 ---
API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_YOUTUBE_TOPIC = "youtubedata"
KAFKA_ERROR_TOPIC = "youtube_token_errors"
ELASTIC_ID = os.getenv("ELASTIC_ID")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_YOUTUBE_INDEX = "youtubedata"

YT_COMMENTS_MAX = int(os.getenv("YT_COMMENTS_MAX", "50"))
YT_COMMENTS_ORDER = os.getenv("YT_COMMENTS_ORDER", "relevance")

logging.basicConfig(level=logging.INFO)

# --- Helper Functions ---
def get_es_client():
    return Elasticsearch(ELASTIC_URL, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))

def get_kafka_producer():
    return Producer({'bootstrap.servers': KAFKA_BROKERS})

def get_youtube_data_api_client():
    return build('youtube', 'v3', developerKey=API_KEY)

def delivery_report(err, msg):
    if err:
        logging.error(f"Kafka delivery failed: {err}")
    else:
        logging.info(f"Kafka message delivered to {msg.topic()} [{msg.partition()}]")

def send_to_kafka(producer, topic, key, message):
    try:
        producer.produce(
            topic=topic,
            key=str(key),
            value=json.dumps(message, ensure_ascii=False),
            callback=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        logging.error(f"Kafka 전송 실패: {e}")

def format_analytics_table(analytics_data, report_name=None):
    headers = [h["name"] for h in analytics_data.get("columnHeaders", [])]
    rows = analytics_data.get("rows", [])
    formatted = []
    for row in rows:
        row_dict = dict(zip(headers, row))
        # demographics 보고서의 viewerPercentage는 무조건 float으로 변환
        if report_name == "demographics" and "viewerPercentage" in row_dict:
            try:
                row_dict["viewerPercentage"] = float(row_dict["viewerPercentage"])
            except Exception:
                pass
        formatted.append(row_dict)
    return formatted

def fetch_all_analytics_reports(access_token, channel_id):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=28)).strftime('%Y-%m-%d')
    analytics_url = "https://youtubeanalytics.googleapis.com/v2/reports"
    headers = {"Authorization": f"Bearer {access_token}"}
    results = {"channel_id": channel_id, "retrieved_date": today}

    report_requests = [
        {"name": "daily_analytics", "params": {"metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,likes,dislikes,shares,comments", "dimensions": "day", "sort": "day"}},
        {"name": "demographics", "params": {"metrics": "viewerPercentage", "dimensions": "ageGroup,gender"}},
        {"name": "traffic_source_analytics", "params": {"metrics": "views,estimatedMinutesWatched", "dimensions": "insightTrafficSourceType"}},
        {"name": "device_type_analytics", "params": {"metrics": "views,estimatedMinutesWatched", "dimensions": "deviceType"}},
        {"name": "os_analytics", "params": {"metrics": "views,estimatedMinutesWatched", "dimensions": "operatingSystem"}},
        {"name": "playback_location_analytics", "params": {"metrics": "views,estimatedMinutesWatched", "dimensions": "insightPlaybackLocationType"}},
        {"name": "top_videos_by_views", "params": {"metrics": "views,likes,shares,comments,estimatedMinutesWatched", "dimensions": "video", "sort": "-views", "maxResults": 25}},
        {"name": "top_videos_by_watch_time", "params": {"metrics": "views,likes,shares,comments,estimatedMinutesWatched", "dimensions": "video", "sort": "-estimatedMinutesWatched", "maxResults": 25}},
        {"name": "country_analytics_by_views", "params": {"metrics": "views,estimatedMinutesWatched", "dimensions": "country"}}
    ]

    for req in report_requests:
        params = {"ids": f"channel=={channel_id}", "startDate": start_date, "endDate": today}
        params.update(req["params"])
        logging.info(f"Fetching report: {req['name']}...")
        resp = requests.get(analytics_url, headers=headers, params=params)
        if resp.status_code == 401:
            logging.error(f"Access token expired for channel {channel_id}")
            return {"error": "token_expired"}
        if resp.status_code != 200:
            logging.error(f"Report '{req['name']}' failed. Status={resp.status_code}, Error={resp.text}")
            results[req['name']] = {"error": f"http_error_{resp.status_code}"}
            continue
        try:
            results[req['name']] = format_analytics_table(resp.json(), report_name=req['name'])
        except Exception as e:
            logging.error(f"Failed to parse report {req['name']}: {e}")
            results[req['name']] = {"error": "parse_error"}
    return results

def extract_thumbnails(snippet):
    thumbs = snippet.get('thumbnails', {}) or {}
    return {k: {"url": v.get("url"), "width": v.get("width"), "height": v.get("height")} for k,v in thumbs.items() if v.get("url")}

# 댓글 및 답글 수집 함수 (로컬 버전 반영)
def fetch_video_comments(youtube_client, video_id, max_comments=50, order="relevance"):
    comments = []
    page_token = None
    while len(comments) < max_comments:
        try:
            req = youtube_client.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=min(100, max_comments-len(comments)),
                pageToken=page_token,
                order=order,
                textFormat="plainText",
            )
            resp = req.execute()
        except Exception as e:
            logging.warning(f"댓글 수집 실패(video_id={video_id}): {e}")
            break
        for it in resp.get("items", []):
            s = it.get("snippet", {}) or {}
            top = (s.get("topLevelComment") or {}).get("snippet", {}) or {}
            thread_id = it.get("id")
            top_id = (s.get("topLevelComment") or {}).get("id") or thread_id
            comment_obj = {
                "thread_id": thread_id,
                "comment_id": top_id,
                "author": top.get("authorDisplayName"),
                "author_channel_id": ((top.get("authorChannelId") or {}).get("value")),
                "text": top.get("textDisplay") or top.get("textOriginal"),
                "like_count": top.get("likeCount"),
                "published_at": top.get("publishedAt"),
                "updated_at": top.get("updatedAt"),
                "reply_count": s.get("totalReplyCount", 0),
                "replies": [],
            }
            comments.append(comment_obj)
        page_token = resp.get("nextPageToken")
        if not page_token: break
    return comments

def fetch_youtube_data(youtube_client, video_id):
    try:
        video_response = youtube_client.videos().list(
            part="snippet,statistics,contentDetails,status,topicDetails,recordingDetails,liveStreamingDetails,localizations",
            id=video_id
        ).execute()
        if not video_response.get('items'):
            return None
        video = video_response['items'][0]
        snippet = video.get('snippet', {})
        stats = video.get('statistics', {})
        return {
            "title": snippet.get('title', ''),
            "description": snippet.get('description', ''),
            "video_id": video.get('id', ''),
            "channel_id": snippet.get('channelId', ''),
            "channel_title": snippet.get('channelTitle', ''),
            "upload_date": snippet.get('publishedAt', ''),
            "tags": snippet.get('tags', []),
            "view_count": stats.get('viewCount'),
            "like_count": stats.get('likeCount'),
            "comment_count": stats.get('commentCount'),
            "thumbnails": extract_thumbnails(snippet),
        }
    except Exception as e:
        logging.error(f"Failed to fetch video data for {video_id}: {e}")
        return None

def index_to_elasticsearch(es_client, data):
    try:
        response = es_client.index(index=ELASTIC_YOUTUBE_INDEX, document=data)
        return response['_id']
    except Exception as e:
        logging.error(f"Elasticsearch 색인 실패: {e}")
        return None

def fetch_all_video_ids(youtube_client, channel_id):
    video_ids = []
    next_page_token = None
    while True:
        search_response = youtube_client.search().list(
            channelId=channel_id,
            part="id",
            order="date",
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in search_response.get('items', []):
            if item.get('id', {}).get('kind') == 'youtube#video':
                video_ids.append(item['id']['videoId'])
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids

# --- Main Task ---
def run_youtube_task(user_id, channel_id, access_token, **kwargs):
    logging.info(f"Starting YouTube task for user: {user_id}")
    es_client = get_es_client()
    kafka_producer = get_kafka_producer()
    youtube_data_client = get_youtube_data_api_client()

    # Analytics API
    channel_analytics = fetch_all_analytics_reports(access_token, channel_id)
    if channel_analytics.get("error") == "token_expired":
        raise Exception(f"YouTube access token expired for user {user_id}.")

    # 영상 목록
    try:
        video_ids_to_process = fetch_all_video_ids(youtube_data_client, channel_id)
        logging.info(f"총 {len(video_ids_to_process)}개의 영상을 가져왔습니다.")
    except Exception as e:
        logging.error(f"Failed to fetch video list for channel {channel_id}: {e}")
        return

    videos = []
    for vid in video_ids_to_process:
        video_data = fetch_youtube_data(youtube_data_client, vid)
        if not video_data: continue
        video_data['comments'] = fetch_video_comments(
            youtube_data_client, vid, max_comments=YT_COMMENTS_MAX, order=YT_COMMENTS_ORDER
        )
        video_data['comments_collected'] = len(video_data['comments'])
        videos.append(video_data)

    channel_document = {
        "channel_id": channel_id,
        "channel_title": (videos[0]["channel_title"] if videos else "unknown_channel"),
        "@timestamp": datetime.utcnow().isoformat(),
        "processed_for_user": user_id,
        "channel_analytics": channel_analytics,
        "videos": videos
    }

    doc_id = index_to_elasticsearch(es_client, channel_document)

    if doc_id:
        kafka_message = {
            "es_doc_id": doc_id,
            "user_id": user_id,
            "channel_id": channel_id,
            "indexed_at": datetime.now().isoformat()
        }
        send_to_kafka(kafka_producer, KAFKA_YOUTUBE_TOPIC, doc_id, kafka_message)

    kafka_producer.flush()
    logging.info(f"Finished YouTube task for user: {user_id}")
