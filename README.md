## Run
```code
cd Extract
python 1.py

cd ../Transform
# Set these in your shell before running spark-submit
export KAFKA_PKG_VERSION="3.5.5"
export MONGO_PKG_VERSION="10.4.0"
export SPARK_KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:${KAFKA_PKG_VERSION}"
export SPARK_MONGO_PKG="org.mongodb.spark:mongo-spark-connector_2.12:${MONGO_PKG_VERSION}"
spark-submit \
  --packages ${SPARK_KAFKA_PKG} \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.streaming.statefulOperator.checkCorrectness.enabled=false \
  1_moving.py
spark-submit \
  --packages ${SPARK_KAFKA_PKG} \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.streaming.statefulOperator.checkCorrectness.enabled=false \
  1_zscore.py

cd ../Load
spark-submit \
  --packages ${SPARK_KAFKA_PKG},${SPARK_MONGO_PKG} \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.streaming.statefulOperator.checkCorrectness.enabled=false \
  1.py
```