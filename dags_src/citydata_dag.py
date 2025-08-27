from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
import json
import requests
from elasticsearch import Elasticsearch
from confluent_kafka import Producer
from dotenv import load_dotenv
import traceback
from kubernetes import client, config

load_dotenv()

# -------------------- 
# 환경 설정
# -------------------- 
API_KEY = os.getenv("SEOUL_API_KEY")
logging.basicConfig(level=logging.INFO)

invalid_codes = {22, 28, 57, 62, 65, 69, 75, 97}
poi_codes = [f"{i:03d}" for i in range(1, 129) if i not in invalid_codes]

BASE_URL = "http://openapi.seoul.go.kr:8088/{API_KEY}/json/citydata/1/5/POI{code}"
RAW_DIR = "/opt/airflow/poi_results"
PROC_DIR = "/opt/airflow/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

ELASTIC_ID = os.getenv("ELASTIC_ID")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
ELASTIC_URL = os.getenv("ELASTIC_URL")

es = Elasticsearch(
    ELASTIC_URL,
    basic_auth=(ELASTIC_ID, ELASTIC_PASSWORD)
)

# -------------------- 
# Kafka delivery callback
# -------------------- 
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"❌ Kafka delivery failed: {err}")
    else:
        logging.info(f"✅ Kafka message delivered: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# -------------------- 
# 유틸
# -------------------- 
def safe_get_nested_list_or_dict(obj, key):
    val = obj.get(key)
    if isinstance(val, list):
        return val
    elif isinstance(val, dict):
        return val
    return None

# -------------------- 
# Pod 삭제 함수
# -------------------- 
def delete_crashloop_pods():
    try:
        # Kubernetes config 로드
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()
        namespace = "dev-system"

        target_keywords = [
            "youtube-pipeline-fetch-and-process-youtube",
            "citydata-pipeline-fetch-and-process-citydata",
            "reddit-pipeline-fetch-and-process-reddit",
            "citydata-pipeline-delete-pod",
            "citydata-pipeline-delete-crashloop-pods"
        ]

        pods = v1.list_namespaced_pod(namespace=namespace)

        for pod in pods.items:
            pod_name = pod.metadata.name
            pod_status = pod.status.phase

            # 키워드 매칭 여부
            if not any(keyword in pod_name for keyword in target_keywords):
                continue

            # CrashLoopBackOff 여부 확인
            backoff_found = False
            if pod.status.container_statuses:
                for cs in pod.status.container_statuses:
                    if cs.state.waiting and "CrashLoopBackOff" in cs.state.waiting.reason:
                        backoff_found = True
                        break
            if not backoff_found:
                continue

            # 삭제 전 로그 출력
            try:
                logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace, tail_lines=50)
                print(f"\n[LOG] {pod_name}\n{'-'*50}\n{logs}\n{'-'*50}")
            except Exception as e:
                print(f"[ERROR] 로그 가져오기 실패 - {pod_name}: {e}")

            # Pod 삭제
            try:
                v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
                print(f"[DELETED] {pod_name} (Status: {pod_status})")
            except Exception as e:
                print(f"[ERROR] 파드 삭제 실패 - {pod_name}: {e}")

    except Exception as e:
        logging.error(f"Pod 삭제 작업 중 오류: {e}")
        logging.error(traceback.format_exc())

# -------------------- 
# Citydata 처리 함수
# -------------------- 
def fetch_process_save_index_produce(poi_code, timestamp_str):
    API_KEY = os.getenv("SEOUL_API_KEY")
    url = BASE_URL.format(API_KEY=API_KEY, code=poi_code)
    json_raw_path = os.path.join(RAW_DIR, f"POI{poi_code}.json")
    json_proc_path = os.path.join(PROC_DIR, f"POI{poi_code}.json")
    producer = Producer({'bootstrap.servers': KAFKA_BROKERS})

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        pri_key = f"{timestamp_str}POI{poi_code}"
        data_with_key = {"es_doc_id": pri_key}
        data_with_key.update(data)

        with open(json_raw_path, "w", encoding="utf-8") as f:
            json.dump(data_with_key, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ [RAW] 저장 완료: {json_raw_path}")

    except Exception as e:
        logging.warning(f"⚠️ POI{poi_code} 요청 실패: {e}")
        return False

    try:
        citydata = data_with_key.get("CITYDATA", {})
        processed_info = {
            "AREA_NM": citydata.get("AREA_NM"),
            "AREA_CD": citydata.get("AREA_CD"),
            "LIVE_PPLTN_STTS": safe_get_nested_list_or_dict(citydata, "LIVE_PPLTN_STTS"),
            "WEATHER_STTS": safe_get_nested_list_or_dict(citydata, "WEATHER_STTS"),
        }
        filtered = {"es_doc_id": pri_key, "citydata": processed_info}

        with open(json_proc_path, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ [PROC] 저장 완료: {json_proc_path}")

    except Exception as e:
        logging.error(f"❌ [PROC] 처리 실패: {e}")
        return False

    try:
        response = es.index(index="citydata", document=filtered)
        doc_id = response.get('_id')
        logging.info(f"✅ Elasticsearch 색인 성공: POI{poi_code}")
    except Exception as e:
        logging.error(f"❌ Elasticsearch 색인 실패: {e}")
        return False

    try:
        kafka_value = {
            "indexed_at": timestamp_str,
            "es_doc_id": doc_id,
            "location": filtered["citydata"].get("AREA_NM"),
            "recorded_at": filtered["citydata"].get("WEATHER_STTS", [{}])[0].get("WEATHER_TIME"),
            "source": "citydata"
        }
        producer.produce(
            topic=KAFKA_TOPIC,
            key=pri_key,
            value=json.dumps(kafka_value, ensure_ascii=False),
            callback=delivery_report
        )
        producer.flush()
        logging.info(f"✅ Kafka 메시지 전송 성공: POI{poi_code}")
    except Exception as e:
        logging.error(f"Kafka 메시지 전송 실패: {e}")
        return False

    return True

# -------------------- 
# 전체 실행
# -------------------- 
def run_all():
    timestamp_str = datetime.now().strftime("%y%m%d%H%M")
    logging.info(f"🔄 {timestamp_str} 기준 데이터 요청 및 처리 시작...")

    for code in poi_codes:
        fetch_process_save_index_produce(code, timestamp_str)
    
    producer = Producer({'bootstrap.servers': KAFKA_BROKERS})

    kafka_value = {
        "indexed_at": timestamp_str,
        "message": "City data to ES Complete"
    }
    producer.produce(
        topic=KAFKA_TOPIC,
        key=timestamp_str,
        value=json.dumps(kafka_value, ensure_ascii=False),
        callback=delivery_report
    )
    producer.flush()
# -------------------- 
# Airflow DAG 정의
# -------------------- 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='citydata_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['seoul', 'citydata']
) as dag:

    task_fetch_citydata = PythonOperator(
        task_id='fetch_and_process_citydata',
        python_callable=run_all
    )
