

import logging
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import praw
from confluent_kafka import Producer
from elasticsearch import Elasticsearch

# .env 파일에서 환경변수 로드
load_dotenv()

# --- 전역 설정 ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "219.255.15.170:9092,219.255.15.170:9093")
KAFKA_REDDIT_TOPIC = "redditdata"
ELASTIC_ID = os.getenv("ELASTIC_ID", "elastic")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "qwer1234")
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "http://219.255.15.170:9200/")
ELASTIC_REDDIT_INDEX = "redditdata"

logging.basicConfig(level=logging.INFO)

# --- Helper Functions ---

def get_es_client():
    return Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))

def get_kafka_producer():
    return Producer({'bootstrap.servers': KAFKA_BROKERS})

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

def index_to_elasticsearch(es_client, data):
    try:
        response = es_client.index(index=ELASTIC_REDDIT_INDEX, document=data)
        return response['_id']
    except Exception as e:
        logging.error(f"Elasticsearch 색인 실패: {e}")
        return None

# --- Main Task Function ---

def run_reddit_task(user_id, client_id, client_secret, user_agent, username, password, **kwargs):
    """한 명의 사용자에 대한 Reddit 데이터 수집 및 처리 Task"""
    logging.info(f"Starting Reddit task for user: {user_id}")

    # 1. 서비스 클라이언트 초기화
    es_client = get_es_client()
    kafka_producer = get_kafka_producer()
    try:
        reddit_client = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password
        )
        # 인증 확인
        logging.info(f"Successfully authenticated Reddit API for user: {reddit_client.user.me()}")
    except Exception as e:
        logging.error(f"Failed to authenticate Reddit API for user {user_id}: {e}")
        # 필요시 Kafka로 인증 실패 알림을 보낼 수 있습니다.
        return

    # 2. 처리할 서브레딧 및 게시물 수 정의
    subreddit_name = "korea" # 예시, 실제로는 사용자별 설정으로 변경 가능
    limit = 20

    # 3. 데이터 수집 및 처리
    try:
        subreddit = reddit_client.subreddit(subreddit_name)
        for submission in subreddit.hot(limit=limit):
            data = {
                "id": submission.id,
                "title": submission.title,
                "score": submission.score,
                "url": submission.url,
                "num_comments": submission.num_comments,
                "created_utc": submission.created_utc,
                "selftext": submission.selftext,
                "subreddit": subreddit_name,
                "processed_for_user": user_id,
                "@timestamp": datetime.utcnow().isoformat()
            }
            
            doc_id = index_to_elasticsearch(es_client, data)

            if doc_id:
                kafka_message = {
                    "es_doc_id": doc_id,
                    "user_id": user_id,
                    "reddit_id": submission.id,
                    "subreddit": subreddit_name,
                    "title": submission.title,
                    "indexed_at": datetime.now().isoformat()
                }
                send_to_kafka(kafka_producer, KAFKA_REDDIT_TOPIC, doc_id, kafka_message)

    except Exception as e:
        logging.error(f"Failed to fetch/process data from Reddit for user {user_id}: {e}")

    finally:
        kafka_producer.flush()
        logging.info(f"Finished Reddit task for user: {user_id}")
