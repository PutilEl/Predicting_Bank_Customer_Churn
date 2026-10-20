# filename=churn_predictions_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main(execution_date):
    # 1. Инициализация Spark-сессии
    spark = SparkSession.builder \
        .appName("ChurnPredictions") \
        .config("fs.s3a.endpoint", "<путь до хранилища>") \
        .getOrCreate()


    # 2. Чтение CSV из S3
    df_csv = spark.read.csv(
        "s3a://da-plus-dags/<логин>/data/churn_predictions_final.csv",
        header=True,          # Есть ли заголовок в CSV
        inferSchema=True      # Нужно ли определять типы данных
    )

    # 3. Настройка подключения к ClickHouse
    jdbcPort = <хост>
    jdbcHostname = "<название_хоста>"
    jdbcDatabase = "<база_данных>"
    jdbc_url = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"
    table_name = "churn_predictions"


    # 4. Запись в ClickHouse через JDBC
    df_csv.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "<логин>") \
        .option("password", "<пароль>") \
        .mode("append") \
        .save()


    # 5. Завершение Spark-сессии
    spark.stop()


if __name__ == "__main__":
    import sys
    # Получение даты запуска из аргумента командной строки
    execution_date = sys.argv[1]
    main(execution_date)