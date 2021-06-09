# Intro to Data_engineering

Построить data pipeline c помощью Spark, HDFS, Airflow:

1. Выгрузите данные из dshop PostgreSQL таблиц в RAW(Bronze).
2. Выгрузите данные на текущий день из out_of_stock API. Если данных на сегодняшний день нет, то Airflow должен выдавать ошибку c соответствующем log message в Логах к Airflow Task.
3. Данные в Bronze должны быть партицированы по дате.
4. Перенесите данные из Bronze в Silver, попутно очистив их и убрав дубликаты.
5. Данные в Silver должны храниться в формате Parquet.
6. Добавьте Airflow логи на каждое действие.
7. Обработка данных из PostgreSQL и из API должна быть в разных Airflow DAG. Каждый отдельный шаг обработки - отдельный Airflow Task.
