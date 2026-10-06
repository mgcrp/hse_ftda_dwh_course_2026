## Семинар 4. Хранение файлов: S3, Hadoop.

### Демо 1 - minio

1. Запустить minio - `docker-compose up -d`
2. Запустить среду и ноутбук - `cd env && sh env_setup.sh`
3. См. `minio_demo.ipynb`

### Демо 2 - hadoop

Ниже - примеры для локального запуска

1. `docker-compose up -d`
Важно: дальше очень долго!
2. `docker cp archive.zip namenode:archive.zip`
3. `docker cp breweries.csv namenode:breweries.csv`
4. `docker exec -it namenode bash`
5. `hdfs dfsadmin -safemode leave`
6. `hdfs dfs -mkdir -p /data/sem_example`
7. `hdfs dfs -ls /data`
8. `hdfs dfs -put archive.zip /data/sem_example/archive.zip`
9. `hdfs fsck /data/sem_example`
10. `hdfs dfs -put breweries.csv /data/sem_example/breweries.csv`

1. `docker exec -it spark-master bash`
2. `/spark/bin/pyspark --master spark://spark-master:7077`
3. `spark`
4. `df = spark.read.csv('hdfs://namenode:9000/data/sem_example/breweries.csv')`
5. `df.show()`
6. Spark
```python
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = spark.read \
    .option('header', 'true') \
    .csv('hdfs://namenode:9000/data/sem_example/breweries.csv')

df.groupby('state') \
    .count() \
    .repartition(1) \
    .write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv('hdfs://namenode:9000/data/sem_example/breweries_groupby_pySpark.csv')
```
7. MapReduce
```bash
docker cp mapper.py namenode:mapper.py
docker cp reducer.py namenode:reducer.py
docker cp pg4300.txt namenode:pg4300.txt
docker cp pg5000.txt namenode:pg5000.txt
```

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
-file mapper.py     -mapper mapper.py \
-file reducer.py    -reducer reducer.py \
-input /data/text/* -output /data/text-output
```

```
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-3.2.2.jar -file mapper.py     -mapper mapper.py -file reducer.py    -reducer reducer.py -input /data/text/* -output /data/text-output
```

Ниже - примеры для yandex cloud dataproc

1. `sudo -u hdfs hdfs dfs -mkdir -p /data/sem_example`
   `sudo -u hdfs hdfs dfs -chown -R ubuntu:hadoop /data`
2. `hdfs dfs -ls /data`
3. `hdfs dfs -put archive.zip /data/sem_example/archive.zip`
4. `hdfs dfs -ls /data/sem_example`
5. `hdfs fsck /data/sem_example`
6. `hdfs dfs -put breweries.csv /data/sem_example/breweries.csv`
7. `hdfs dfs -ls /data/sem_example`
8. `hdfs fsck /data/sem_example`
9. `/usr/bin/pyspark`
```python
df = spark \
    .read \
    .option('header', 'true') \
    .csv('hdfs://rc1d-dataproc-m-lklbj782udualp1r.mdb.yandexcloud.net:8020/data/sem_example/breweries.csv')
```
11.
```python
df.groupby('state') \
    .count() \
    .repartition(1) \
    .write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv('hdfs://rc1d-dataproc-m-lklbj782udualp1r.mdb.yandexcloud.net:8020/data/sem_example/breweries_groupby_pySpark.csv')
```
12.
```python
spark.read.option('header', 'true').csv('hdfs://rc1d-dataproc-m-lklbj782udualp1r.mdb.yandexcloud.net:8020/data/sem_example/breweries_groupby_pySpark.csv').show()
```
1.  `hdfs dfs -mkdir -p /data/text`
2.  `hdfs dfs -put pg4300.txt /data/text/pg4300.txt`
3.  `hdfs dfs -put pg5000.txt /data/text/pg5000.txt`
4.  `hdfs dfs -ls /data/text`
5.  
```bash
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /data/text/* \
  -output /data/text-output
```
18.
```python
df = spark.read.csv('hdfs://rc1d-dataproc-m-lklbj782udualp1r.mdb.yandexcloud.net:8020/data/text-output', sep='\t')

df.sort(df._c1, ascending=False).show()
```