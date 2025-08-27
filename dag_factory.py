import json
import os
from pathlib import Path

# --- 설정 ---
# 이 스크립트 파일의 위치를 기준으로 경로를 설정합니다.
BASE_DIR = Path(__file__).resolve().parent
USERS_CONFIG_FILE = BASE_DIR / 'service_users.json'
GENERATED_DAGS_DIR = BASE_DIR / 'dags'

# --- DAG 파일 템플릿 ---
# 동적으로 생성될 DAG 파일의 기본 구조입니다.
DAG_TEMPLATE = """
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Task 모듈 임포트
from tasks.youtube_task import run_youtube_task
from tasks.reddit_task import run_reddit_task

with DAG(
    dag_id=\"{user_id}_services_dag\",
    schedule=\"{schedule}\",
    start_date=pendulum.datetime(2024, 1, 1, tz=\"UTC\"),
    catchup=False,
    tags=[\"dynamic-user-dag\", \"{user_id}\"]
) as dag:

{tasks}
"""

# --- Task 템플릿 ---
# 각 서비스에 대한 PythonOperator Task를 생성하는 템플릿입니다.
YOUTUBE_TASK_TEMPLATE = """
    youtube_task = PythonOperator(
        task_id=\"youtube_data_pipeline\",
        python_callable=run_youtube_task,
        op_kwargs={op_kwargs}
    )
"""

REDDIT_TASK_TEMPLATE = """
    reddit_task = PythonOperator(
        task_id=\"reddit_data_pipeline\",
        python_callable=run_reddit_task,
        op_kwargs={op_kwargs}
    )
"""

def create_dag_file(user_config: dict):
    """사용자 설정 한 개를 받아 DAG 파일을 생성합니다."""
    user_id = user_config["user_id"]
    schedule = user_config.get("schedule", "@daily") # 기본값은 daily
    
    tasks_code = []
    
    # YouTube 서비스가 활성화되어 있으면 Task를 추가합니다.
    if "youtube" in user_config["services"]:
        yt_config = user_config["services"]["youtube"]
        yt_config['user_id'] = user_id # op_kwargs에 user_id 추가
        op_kwargs_str = json.dumps(yt_config)
        tasks_code.append(YOUTUBE_TASK_TEMPLATE.format(op_kwargs=op_kwargs_str))

    # Reddit 서비스가 활성화되어 있으면 Task를 추가합니다.
    if "reddit" in user_config["services"]:
        rd_config = user_config["services"]["reddit"]
        rd_config['user_id'] = user_id # op_kwargs에 user_id 추가
        op_kwargs_str = json.dumps(rd_config)
        tasks_code.append(REDDIT_TASK_TEMPLATE.format(op_kwargs=op_kwargs_str))

    if not tasks_code:
        print(f"사용자 {user_id}에게 활성화된 서비스가 없어 DAG를 생성하지 않습니다.")
        return

    # 전체 DAG 코드를 조합합니다.
    # 여기서는 Task 간의 의존성 없이 병렬로 실행되도록 설정합니다.
    full_dag_code = DAG_TEMPLATE.format(
        user_id=user_id,
        schedule=schedule,
        tasks="\n".join(tasks_code)
    )

    # 생성된 코드를 파일로 저장합니다.
    dag_file_path = GENERATED_DAGS_DIR / f"{user_id}_services_dag.py"
    with open(dag_file_path, "w") as f:
        f.write(full_dag_code)
    
    print(f"성공적으로 {dag_file_path} 파일을 생성했습니다.")

def main():
    """메인 함수: 사용자 설정 파일을 읽고 모든 사용자에 대한 DAG를 생성합니다."""
    print("DAG 팩토리 실행을 시작합니다...")
    try:
        with open(USERS_CONFIG_FILE, 'r') as f:
            all_users_config = json.load(f)
    except FileNotFoundError:
        print(f"오류: 사용자 설정 파일({USERS_CONFIG_FILE})을 찾을 수 없습니다.")
        return

    for user_config in all_users_config:
        create_dag_file(user_config)
    
    print("DAG 팩토리 실행을 완료했습니다.")

if __name__ == "__main__":
    main()
