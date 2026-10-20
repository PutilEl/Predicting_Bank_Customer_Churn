# filename=predictions_daily.py

from datetime import datetime
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.yandex.operators.dataproc import DataprocCreatePysparkJobOperator

class PysparkJobOperator(DataprocCreatePysparkJobOperator):
	template_fields = ('cluster_id', 'args')
    
DAG_ID = "churn_predictions_final"    

with DAG(
    DAG_ID,                                                # 1. ID DAG 
    description = "Модель предсказания оттока клиентов" ,  # 2. Краткое описание 
    schedule_interval = "@daily",                          # 3. Расписание
    start_date = datetime(2026, 10, 5),                    # 4. Дата старта 
    catchup = False,                                       # 5. True/False: запускать для прошлых дат?
    tags=["churn", "predictions"]                          # 6. Список тегов
) as dag:
    
    user = '<логин>'

    wait_for_input = S3KeySensor(
        task_id = "wait_for_input",
        bucket_name = "da-plus-dags",                            # 7. 'da-plus-dags'
        bucket_key = f"{user}/data/churn_predictions_final.csv", # 8. '<логин>/data/churn_predictions_final.csv'
        aws_conn_id = "s3",                                      # 9. 's3' — подключение уже настроено в окружении
        poke_interval = 60,                                      # 10. Интервал проверки
        timeout = 3600,                                          # 11. Тайм-аут ожидания
        mode = "poke",
        wildcard_match = False
    )

    run_pyspark = PysparkJobOperator(
        task_id = "run_pyspark_job",
        name = "daily_pyspark_job",                 # 12. Имя задания в Dataproc
        cluster_id = "<ID_кластера_Dataproc >",     # 13. ID кластера Dataproc 
        args = ["{{ ds }}"] ,                       # 14. Аргументы для скрипта
        main_python_file_uri = f"s3a://da-plus-dags/{user}/jobs/churn_predictions_job.py" # Путь для доступа к данным из хранилища s3

# 16. Зададим зависимость: wait_for_input >> run_pyspark
wait_for_input >> run_pyspark