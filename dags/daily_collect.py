"""
일간 데이터 수집 DAG (Airflow)
- 매일 장 마감 후 실행 (16:00 KST)
- 주가, 거래량, 시가총액 수집
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# 기본 DAG 설정
default_args = {
    'owner': 'cufa',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_collect',
    default_args=default_args,
    description='일간 주가/재무 데이터 수집',
    schedule_interval='0 16 * * 1-5',  # 평일 16:00 KST
    catchup=False,
    tags=['cufa', 'daily', 'collect']
)


def collect_daily_prices(**context):
    """일간 주가 수집"""
    from collectors.async_price import collect_all_marketcap
    import asyncio
    
    print("📈 일간 주가 수집 시작...")
    df = asyncio.run(collect_all_marketcap())
    print(f"✓ {len(df)}개 종목 수집 완료")
    
    # XCom으로 결과 전달
    context['ti'].xcom_push(key='price_count', value=len(df))
    return len(df)


def calculate_technical_indicators(**context):
    """기술적 지표 계산"""
    print("📊 기술적 지표 계산...")
    # TODO: 이동평균, RSI, MACD 등
    return True


def collect_dart_disclosure(**context):
    """DART 공시 수집"""
    print("📋 DART 공시 수집...")
    # TODO: 오늘 공시된 재무제표 수집
    return True


def update_ratios(**context):
    """재무비율 갱신"""
    print("💹 재무비율 갱신...")
    # TODO: 신규 데이터로 비율 재계산
    return True


def send_daily_report(**context):
    """일일 리포트 발송"""
    price_count = context['ti'].xcom_pull(key='price_count', task_ids='collect_prices')
    
    report = f"""
    📊 CUFA Daily Report
    ━━━━━━━━━━━━━━━━━━━━
    수집 종목: {price_count}개
    실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M')}
    ✓ 모든 작업 완료
    """
    print(report)
    
    # TODO: Telegram 또는 Slack 발송
    return True


# 태스크 정의
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

collect_prices = PythonOperator(
    task_id='collect_prices',
    python_callable=collect_daily_prices,
    dag=dag
)

calc_indicators = PythonOperator(
    task_id='calc_indicators',
    python_callable=calculate_technical_indicators,
    dag=dag
)

collect_disclosure = PythonOperator(
    task_id='collect_disclosure',
    python_callable=collect_dart_disclosure,
    dag=dag
)

update_ratio = PythonOperator(
    task_id='update_ratios',
    python_callable=update_ratios,
    dag=dag
)

daily_report = PythonOperator(
    task_id='daily_report',
    python_callable=send_daily_report,
    dag=dag
)

# 워크플로우
start >> collect_prices >> calc_indicators >> collect_disclosure >> update_ratio >> daily_report >> end
