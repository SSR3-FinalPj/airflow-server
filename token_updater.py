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
    'auto.offset.reset': 'latest',
}
KAFKA_TOPIC = KAFKA_GOOGLE_TOPIC


def update_user_config(youtube_channel_id: str, new_user_id: int, new_access_token: str) -> bool:
    """
    youtube_channel_id를 기준으로 사용자를 찾아 user_id와 access_token을 업데이트합니다.
    """
    try:
        with open(USERS_CONFIG_FILE, 'r+') as f:
            all_users_config = json.load(f)
            
            user_found = False
            for user_config in all_users_config:
                if 'youtube' in user_config.get('services', {}) and \
                   user_config['services']['youtube'].get('channel_id') == youtube_channel_id:
                    
                    logging.info(f"Found user for channel_id '{youtube_channel_id}'. Updating user_id and token.")
                    # user_id와 access_token을 모두 업데이트
                    user_config['user_id'] = new_user_id
                    user_config['services']['youtube']['access_token'] = new_access_token
                    user_found = True
                    break
            
            if not user_found:
                logging.warning(f"No user found for youtube_channel_id: {youtube_channel_id}")
                return False

            f.seek(0)
            json.dump(all_users_config, f, indent=4)
            f.truncate()
            logging.info(f"Successfully updated config in {USERS_CONFIG_FILE}")
            return True

    except FileNotFoundError:
        logging.error(f"Config file not found: {USERS_CONFIG_FILE}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while updating config: {e}")
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
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaException._PARTITION_EOF:
                    logging.error(f"Kafka error: {msg.error()}")
                continue
            
            try:
                payload = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {json.dumps(payload, indent=2)}")

                user_id = payload.get('userId')
                access_token = payload.get('accessToken')
                channel_id = payload.get('youtubeChannelId')

                if not all([user_id, access_token, channel_id]):
                    logging.warning("Message is missing 'userId', 'accessToken', or 'youtubeChannelId'. Skipping.")
                    continue
                
                if update_user_config(channel_id, user_id, access_token):
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