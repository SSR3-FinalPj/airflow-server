import json
import os
import subprocess
import logging
from confluent_kafka import Consumer, KafkaException

# --- 기본 설정 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_CONFIG_FILE = os.path.join(BASE_DIR, 'service_users.json')
DAG_FACTORY_SCRIPT = os.path.join(BASE_DIR, 'dag_factory.py')
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_GOOGLE_TOPIC = os.getenv("KAFKA_GOOGLE_TOPIC")

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BROKERS,
    'group.id': 'airflow-token-updater-group',
    'auto.offset.reset': 'earliest',
}
KAFKA_TOPIC = KAFKA_GOOGLE_TOPIC


def update_user_config(payload: dict) -> bool:
    """
    Kafka payload 기반으로 service_users.json 업데이트
    - user_id 없으면 추가
    - provider 없으면 추가
    - 있으면 access_token, channel_id 업데이트
    """
    user_id = payload.get("userId")
    provider = payload.get("provider")
    access_token = payload.get("accessToken")
    channel_id = payload.get("youtubeChannelId")  # 실제로는 공통 channelId

    if not all([user_id, provider, access_token, channel_id]):
        logging.warning("Payload missing required fields. Skipping.")
        return False

    # provider 변환: google -> youtube
    if provider == "google":
        provider_key = "youtube"
    else:
        provider_key = provider

    try:
        # JSON 파일 로드 (없으면 빈 리스트)
        if not os.path.exists(USERS_CONFIG_FILE):
            all_users_config = []
        else:
            with open(USERS_CONFIG_FILE, 'r') as f:
                all_users_config = json.load(f)

        # user_id 찾기
        user_config = next((u for u in all_users_config if u["user_id"] == user_id), None)

        if not user_config:
            # 새로운 유저 추가
            logging.info(f"Adding new user_id={user_id} with provider={provider_key}")
            user_config = {
                "user_id": user_id,
                "schedule": "@hourly",
                "services": {
                    provider_key: {
                        "channel_id": channel_id,
                        "access_token": access_token
                    }
                }
            }
            all_users_config.append(user_config)
        else:
            # 기존 유저 → provider 확인
            if provider_key not in user_config["services"]:
                logging.info(f"Adding new provider={provider_key} for user_id={user_id}")
                user_config["services"][provider_key] = {}

            logging.info(f"Updating {provider_key} service for user_id={user_id}")
            user_config["services"][provider_key]["channel_id"] = channel_id
            user_config["services"][provider_key]["access_token"] = access_token

        # 다시 저장
        with open(USERS_CONFIG_FILE, 'w') as f:
            json.dump(all_users_config, f, indent=4)

        logging.info(f"Successfully updated config in {USERS_CONFIG_FILE}")
        return True

    except Exception as e:
        logging.error(f"Error updating user config: {e}")
        return False


def regenerate_dags():
    """
    dag_factory.py를 실행하여 DAG 파일을 다시 생성합니다.
    """
    try:
        logging.info(f"Running DAG factory script: {DAG_FACTORY_SCRIPT}")
        result = subprocess.run(['python3', DAG_FACTORY_SCRIPT], capture_output=True, text=True, check=True)
        logging.info("DAG factory script executed successfully.")
        logging.info(f"Stdout: {result.stdout}")
        if result.stderr:
            logging.warning(f"Stderr: {result.stderr}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while running DAG factory: {e}")


def main():
    """
    Kafka 메시지를 수신하여 토큰 업데이트 및 DAG 재생성을 수행하는 메인 함수
    """
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"Listening to topic '{KAFKA_TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaException._PARTITION_EOF:
                    logging.error(f"Kafka error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {json.dumps(payload, indent=2)}")

                if update_user_config(payload):
                    regenerate_dags()

            except Exception as e:
                logging.error(f"An error occurred in message processing loop: {e}")

    except KeyboardInterrupt:
        logging.info("Stopping consumer...")
    finally:
        consumer.close()
        logging.info("Consumer closed.")


if __name__ == '__main__':
    main()
