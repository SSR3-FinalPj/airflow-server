import os
import json
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from confluent_kafka import Producer

# --- 환경 변수 로드 ---
load_dotenv()

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO)

# --- Elasticsearch 설정 ---
ELASTIC_ID = os.getenv("ELASTIC_ID")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_URL = os.getenv("ELASTIC_URL")
INDEX_NAME = "redditdata"

# ES 클라이언트
es = Elasticsearch(ELASTIC_URL, basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD))

# --- Kafka 설정 ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_TOPIC = "redditdata"

# --- Reddit API 호출 헬퍼 ---
def reddit_get(endpoint, token, params=None):
    headers = {
        "Authorization": f"bearer {token}",
        "User-Agent": "our-service/1.0"
    }
    url = f"https://oauth.reddit.com{endpoint}"
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        logging.error(f"Reddit API 호출 실패: {resp.status_code} {resp.text}")
        return None
    return resp.json()

# --- 댓글 트리 파싱 ---
def parse_comments(comments, depth=0):
    parsed = []
    for c in comments:
        if c["kind"] != "t1":
            continue
        data = c["data"]
        replies = []
        if data.get("replies") and isinstance(data["replies"], dict):
            replies = parse_comments(data["replies"]["data"]["children"], depth + 1)

        data["parsed_replies"] = replies
        if 'replies' in data:
            del data['replies']
        parsed.append(data)
    return parsed

# --- Kafka 딜리버리 콜백 ---
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Kafka delivery failed: {err}")
    else:
        logging.info(f"Kafka message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

# --- 메인 로직 (Airflow Task에서 실행할 함수) ---
def run_reddit_task(user_id, reddit_username, access_token, **kwargs):
    """
    특정 사용자에 대한 Reddit 데이터 수집 및 처리 Task
    """
    logging.info(f"Fetching all data for user: {reddit_username} (user_id: {user_id})")
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
    all_posts_data = []
    after = None

    # 1. 페이지네이션을 통해 모든 게시물과 댓글 수집
    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after

        submissions = reddit_get(f"/user/{reddit_username}/submitted", access_token, params=params)
        if not submissions or not submissions.get("data", {}).get("children"):
            logging.info("No more submissions found.")
            break

        for item in submissions["data"]["children"]:
            if item["kind"] != "t3":
                continue
            s = item["data"]

            comments_raw = reddit_get(f"/comments/{s['id']}", access_token, params={"limit": 500})
            comments_data = []
            if comments_raw and len(comments_raw) > 1:
                comments_data = parse_comments(comments_raw[1]["data"]["children"])

            s["parsed_comments"] = comments_data
            all_posts_data.append(s)
            logging.info(f"Collected post: {s['id']}")

        after = submissions["data"].get("after")
        if not after:
            break

    if not all_posts_data:
        logging.info(f"No data collected for user {reddit_username}. Nothing to process.")
        return

    # 2. 단일 문서 집계
    final_document = {
        "user_id": user_id,
        "reddit_username": reddit_username,
        "retrieved_at": datetime.now().isoformat(),
        "post_count": len(all_posts_data),
        "posts": all_posts_data,
    }

    # 3. Elasticsearch에 색인
    try:
        resp = es.index(index=INDEX_NAME, document=final_document)
        doc_id = resp["_id"]
        logging.info(f"Successfully indexed single document for user {reddit_username}. ES doc ID: {doc_id}")
    except Exception as e:
        logging.error(f"Elasticsearch indexing failed for user {reddit_username}: {e}")
        return

    # 4. Kafka 메시지 전송
    try:
        message = {
            "es_doc_id": doc_id,
            "user_id": user_id,
            "channel_id": reddit_username,
            "indexed_at": datetime.now().isoformat(),
        }
        producer.produce(
            topic=KAFKA_TOPIC,
            key=str(doc_id),
            value=json.dumps(message, ensure_ascii=False),
            callback=delivery_report,
        )
        producer.poll(0)
        producer.flush()
        logging.info(f"Successfully sent Kafka message for user {reddit_username}")
    except Exception as e:
        logging.error(f"Kafka produce failed for user {reddit_username}: {e}")

    logging.info(
        f"완료: 총 {len(all_posts_data)}개 게시글 데이터를 단일 문서로 처리 및 전송 완료 (사용자: {reddit_username})"
    )
