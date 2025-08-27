import logging
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
from googleapiclient.discovery import build
from confluent_kafka import Producer
from elasticsearch import Elasticsearch

# .env 파일에서 환경변수 로드
load_dotenv()

# --- 전역 설정 ---
API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "219.255.15.170:9092,219.255.15.170:9093")
KAFKA_YOUTUBE_TOPIC = "youtubedata"
KAFKA_ERROR_TOPIC = "youtube_token_errors"
ELASTIC_ID = os.getenv("ELASTIC_ID", "elastic")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "qwer1234")
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "http://219.255.15.170:9200/")
ELASTIC_YOUTUBE_INDEX = "youtubedata"

logging.basicConfig(level=logging.INFO)

# --- Helper Functions ---

def get_es_client():
    return Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))

def get_kafka_producer():
    return Producer({'bootstrap.servers': KAFKA_BROKERS})

def get_youtube_data_api_client():
    return build('youtube', 'v3', developerKey=API_KEY)

def delivery_report(err, msg):
    if err is not None:
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

def format_analytics_table(analytics_data):
    """columnHeaders / rows 형태를 [{col: value, ...}, ...] 로 변환"""
    headers = [h["name"] for h in analytics_data.get("columnHeaders", [])]
    rows = analytics_data.get("rows", [])
    return [dict(zip(headers, row)) for row in rows]

def fetch_all_analytics_reports(access_token, channel_id):
    """AccessToken으로 YouTube Analytics API 호출"""
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
        params = {
            "ids": f"channel=={channel_id}",
            "startDate": start_date,
            "endDate": today,
        }
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
            results[req['name']] = format_analytics_table(resp.json())
        except Exception as e:
            logging.error(f"Failed to parse report {req['name']}: {e}")
            results[req['name']] = {"error": "parse_error"}

    return results

def fetch_youtube_data(youtube_client, video_id):
    video_response = youtube_client.videos().list(part='snippet,statistics,id', id=video_id).execute()
    if not video_response.get('items'):
        return None
    video = video_response['items'][0]
    snippet, stats = video.get('snippet', {}), video.get('statistics', {})
    return {
        "title": snippet.get('title', ''),
        "video_id": video.get('id', ''),
        "channel_id": snippet.get('channelId', ''),
        "channel_title": snippet.get('channelTitle', ''),
        "upload_date": snippet.get('publishedAt', ''),
        "view_count": stats.get('viewCount', 'N/A'),
        "like_count": stats.get('likeCount', 'N/A'),
        "comment_count": stats.get('commentCount', 'N/A'),
    }

def index_to_elasticsearch(es_client, data):
    try:
        response = es_client.index(index=ELASTIC_YOUTUBE_INDEX, document=data)
        return response['_id']
    except Exception as e:
        logging.error(f"Elasticsearch 색인 실패: {e}")
        return None

# --- Main Task Function ---

def run_youtube_task(user_id, channel_id, access_token, **kwargs):
    """한 명의 사용자에 대한 유튜브 데이터 수집 및 처리 Task"""
    logging.info(f"Starting YouTube task for user: {user_id}")
    
    # 1. 서비스 클라이언트 초기화
    es_client = get_es_client()
    kafka_producer = get_kafka_producer()
    youtube_data_client = get_youtube_data_api_client()

    # 2. YouTube Analytics API (requests 기반)
    all_analytics_reports = fetch_all_analytics_reports(access_token, channel_id)

    if all_analytics_reports.get("error") == "token_expired":
        error_message = {"user_id": user_id, "service": "youtube", "error": "access_token_expired", "timestamp": datetime.now().isoformat()}
        send_to_kafka(kafka_producer, KAFKA_ERROR_TOPIC, user_id, error_message)
        kafka_producer.flush()
        raise Exception(f"YouTube access token for user {user_id} has expired.")
    
    # 3. 처리할 동영상 목록 가져오기
    try:
        search_response = youtube_data_client.search().list(
            channelId=channel_id,
            part="id",
            order="date",
            maxResults=5
        ).execute()
        video_ids_to_process = [
            item['id']['videoId'] 
            for item in search_response.get('items', []) 
            if item.get('id', {}).get('kind') == 'youtube#video' and 'videoId' in item.get('id', {})
        ]
        logging.info(f"Found recent videos for user {user_id}: {video_ids_to_process}")
    except Exception as e:
        logging.error(f"Failed to fetch video list for channel {channel_id}: {e}")
        return

    # 4. 각 동영상 데이터 처리
    for vid in video_ids_to_process:
        logging.info(f"Processing video_id={vid} for user={user_id}")
        data = fetch_youtube_data(youtube_data_client, vid)
        if not data:
            continue
        
        # Add the comprehensive channel analytics to each video document
        data['channel_analytics'] = all_analytics_reports
        data['processed_for_user'] = user_id
        data['@timestamp'] = datetime.utcnow().isoformat()

        doc_id = index_to_elasticsearch(es_client, data)

        if doc_id:
            kafka_message = {
                "es_doc_id": doc_id,
                "user_id": user_id,
                "youtube_id": data.get("video_id", ""),
                "title": data.get("title", ""),
                "indexed_at": datetime.now().isoformat(),
                "comment_count": data.get("comment_count", 0),
                "like_count": data.get("like_count", 0),
                "view_count": data.get("view_count", 0),
                "published_at": data.get("upload_date", ""),
            }
            send_to_kafka(kafka_producer, KAFKA_YOUTUBE_TOPIC, doc_id, kafka_message)

    kafka_producer.flush()
    logging.info(f"Finished YouTube task for user: {user_id}")
