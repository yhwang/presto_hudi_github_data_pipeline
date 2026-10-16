#Prep

## Git clone project
```sh
git clone https://github.com/saurabhmahawar/presto_hudi_github_data_pipeline.git
cd presto_hudi_github_data_pipeline
```

## Pull images
```sh
cp .env.example .env
docker compose pull
```

# Check minio file usage:

```sh
docker exec -ti minio sh -c "du -h -d 1 /data/github-raw-data/github-raw"

docker exec -ti minio sh -c "du -h -d 1 /data/github-raw-data/hudi-tables/github_events"
```

# Hudi ACID demo

## launch pyspark

```sh
docker exec spark-client pip install ipython
docker exec -it spark-client bash -c "PYSPARK_DRIVER_PYTHON=ipython /opt/spark/bin/pyspark \
  --master "local[4]" --driver-memory 2G"
```

load the libraries:

```python
import os, datetime
from pyspark.sql.functions import col, lit

bucket_name = os.environ.get("S3_BUCKET_NAME", "github-raw-data")
table_path = f"s3a://{bucket_name}/hudi-tables/github_events"

df = spark.read.format("hudi").load(table_path)
df.count()
```

show how many commits:
```python
df.select("_hoodie_commit_time").distinct().show()
```

demo 1:
```python
sample_row = df.filter(col("type").isin("PullRequestEvent", "IssuesEvent")).limit(1).collect()
if sample_row:
    target_id = sample_row[0]["id"]
    target_type = sample_row[0]["type"]
    original_ts = sample_row[0]["created_at"]
    print(f"Target Event ID: {target_id} (Type: {target_type}, Original created_at: {original_ts})")
    new_ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    update_df = df.filter(col("id") == target_id) \
                    .withColumn("created_at", lit(new_ts))
    hudi_upsert_options = {
        'hoodie.table.name': 'github_events',
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.partitionpath.field': 'type',
        'hoodie.datasource.write.precombine.field': 'created_at',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.metadata.enable': 'false',
        'hoodie.avro.schema.validate': 'false',
        'hoodie.datasource.write.reconcile.schema': 'true',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.database': 'default',
        'hoodie.datasource.hive_sync.table': 'github_events',
        'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083'
    }
    print("Writing ACID UPSERT batch...")
    update_df.write.format("hudi") \
                .options(**hudi_upsert_options) \
                .mode("append") \
                .save(table_path)
    print("✅ Upsert completed! Record updated in-place without table rewrite or duplicates.")
```

verify demo 1:

show all commits first
```python
df.filter("type='PullRequestEvent'").groupBy("_hoodie_file_name","_hoodie_commit_time").count().orderBy('_hoodie_commit_time').show(truncate=False)
```

reload the df and check the commit:
```python
df = spark.read.format("hudi") \
    .option("as.of.instant", "20261015162839804") \
    .load(table_path)

df.filter("id = '16927655166'") \
  .select("id","type","created_at","_hoodie_commit_time","_hoodie_file_name").show(truncate=False)

```

demo 2:
```python
df = spark.read.format("hudi").load(table_path)
user_sample = df.select("actor.login", "id", "type").limit(1).collect()
if user_sample:
    user_to_delete = user_sample[0]["login"]
    print(f"Deleting all records for user: '{user_to_delete}'...")
    delete_keys_df = df.filter(col("actor.login") == user_to_delete).select("id", "type", "created_at")
    deleted_count = delete_keys_df.count()
    print(f"Found {deleted_count} records matching user '{user_to_delete}'.")
    hudi_delete_options = {
        'hoodie.table.name': 'github_events',
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.partitionpath.field': 'type',
        'hoodie.datasource.write.operation': 'delete',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.metadata.enable': 'false',
        'hoodie.avro.schema.validate': 'false',
        'hoodie.datasource.write.reconcile.schema': 'true',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.database': 'default',
        'hoodie.datasource.hive_sync.table': 'github_events',
        'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083'
    }
    delete_keys_df.write.format("hudi") \
                    .options(**hudi_delete_options) \
                    .mode("append") \
                    .save(table_path)
    print(f"✅ GDPR point deletion completed! All records for '{user_to_delete}' removed.")
```

verify:
```python
spark.read.format("hudi").load(table_path).filter(col("actor.login") == user_to_delete).count()
```

demo 3:
```python
commits_df = spark.read.format("hudi").load(table_path) \
                    .select("_hoodie_commit_time") \
                    .distinct() \
                    .orderBy(col("_hoodie_commit_time").desc())

print("Recent Lakehouse Commit Instants:")
commits_df.show(truncate=False)

old = spark.read.format("hudi").option("as.of.instant", "20261015000024570").load(table_path)
old.filter(col("actor.login") == user_to_delete).count()
```

use superset sql tab:
```sql
SELECT "_hoodie_commit_time", count(*) AS row_count
FROM github_events
GROUP BY 1 ORDER BY 1;
```


Spark SQL:
```sh
docker exec -it spark-client /opt/spark/bin/spark-sql \
  --master "local[4]" --driver-memory 2G \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --hiveconf hive.cli.print.header=true
```

```python
spark.sql("CALL show_commits(table => 'github_events', limit => 10)").show(truncate=False)
```

```python
spark.sql("SELECT id, created_at, _hoodie_commit_time FROM github_events WHERE id = '16927655166'").show(truncate=False)
```

```python
df = spark.read.format("hudi") \
    .option("as.of.instant", "20261013000126583") \
    .load(table_path)

df.createOrReplaceTempView("github_events_snapshot")

spark.sql("SELECT id, created_at, _hoodie_commit_time \
FROM github_events_snapshot \
WHERE id = '16927655166'").show(truncate=False)
```

```python
df = spark.read.format("hudi") \
    .option("as.of.instant", "20261013033650168") \
    .load(table_path)

df.createOrReplaceTempView("github_events_snapshot")

spark.sql("SELECT id, type FROM github_events_snapshot \
WHERE actor.login = 'COLMACpulse'").show(truncate=False)
```

```python
spark.sql("SELECT id, type FROM github_events \
WHERE actor.login = 'COLMACpulse'").show(truncate=False)
```

```python
df = spark.read.format("hudi") \
    .option("as.of.instant", "20261012012926220") \
    .load(table_path)

df.createOrReplaceTempView("github_events_snapshot")

spark.sql("""
    SELECT count(*)
    FROM github_events_snapshot
    WHERE type = 'PullRequestEvent'
""").show(truncate=False)

df.filter("type = 'PullRequestEvent'").count()
```

how many records in each batch:
```sql
SELECT _hoodie_commit_time, count(*) AS records_in_batch
FROM github_events
GROUP BY 1
ORDER BY 1 DESC;
```