import json
import os
from datetime import datetime, timezone

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    expr,
    first,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    stddev,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    udf,
    when,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructType


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value or ""


TRADES_TOPIC = env("KAFKA_TRADES_TOPIC", "trades_topic")
NEWS_TOPIC = env("KAFKA_NEWS_TOPIC", "news_topic")
FRED_TOPIC = env("KAFKA_FRED_TOPIC", "fred_topic")
ALERTS_TOPIC = env("KAFKA_ALERTS_TOPIC", "alerts_topic")
CHECKPOINT_PATH = env("SPARK_CHECKPOINT_PATH", "/tmp/crypto-intelligence/checkpoints/main")
DATA_LAKE_PATH = env("DATA_LAKE_PATH", "/tmp/crypto-intelligence/data-lake/enriched")


def kafka_options() -> dict[str, str]:
    return {
        "kafka.bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", required=True),
        "kafka.security.protocol": env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "kafka.sasl.mechanism": env("KAFKA_SASL_MECHANISMS", "PLAIN"),
        "kafka.sasl.jaas.config": (
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{env("KAFKA_SASL_USERNAME", required=True)}" '
            f'password="{env("KAFKA_SASL_PASSWORD", required=True)}";'
        ),
    }


def simple_sentiment(text: str | None) -> float:
    if not text:
        return 0.0
    lower = text.lower()
    positive = ["gain", "gains", "rally", "bull", "surge", "up", "record", "approve"]
    negative = ["loss", "losses", "bear", "drop", "down", "crash", "selloff", "risk"]
    return float(sum(word in lower for word in positive) - sum(word in lower for word in negative))


sentiment_udf = udf(simple_sentiment, DoubleType())

trade_schema = (
    StructType()
    .add("symbol", StringType())
    .add("price", DoubleType())
    .add("quantity", DoubleType())
    .add("trade_id", LongType())
    .add("event_time", StringType())
    .add("ingestion_time", StringType())
    .add("source", StringType())
)

news_schema = (
    StructType()
    .add("title", StringType())
    .add("description", StringType())
    .add("url", StringType())
    .add("source", StringType())
    .add("published_at", StringType())
    .add("ingestion_time", StringType())
)

fred_schema = (
    StructType()
    .add("series_id", StringType())
    .add("series_name", StringType())
    .add("date", StringType())
    .add("value", DoubleType())
    .add("source", StringType())
    .add("ingestion_time", StringType())
)


def read_topic(spark: SparkSession, topic: str):
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", topic)
        .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "latest"))
        .load()
    )


def redis_client():
    password = env("REDIS_PASSWORD")
    return redis.Redis(
        host=env("REDIS_HOST", "localhost"),
        port=int(env("REDIS_PORT", "6379")),
        password=password or None,
        decode_responses=True,
    )


def write_to_redis(rows) -> None:
    client = redis_client()
    prefix = env("REDIS_KEY_PREFIX", "crypto:intelligence")
    for row in rows:
        payload = row.asDict(recursive=True)
        symbol = payload.get("symbol", "market")
        client.set(f"{prefix}:{symbol}:latest", json.dumps(payload, default=str), ex=3600)


def write_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    enriched = batch_df.withColumn("batch_id", lit(batch_id)).withColumn("processed_at", current_timestamp())

    enriched.write.mode("append").partitionBy("symbol").parquet(DATA_LAKE_PATH)

    jdbc_url = env("SUPABASE_JDBC_URL")
    if jdbc_url:
        (
            enriched.write.mode("append")
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", env("SUPABASE_TABLE", "crypto_market_intelligence"))
            .option("user", env("SUPABASE_DB_USER", required=True))
            .option("password", env("SUPABASE_DB_PASSWORD", required=True))
            .option("driver", "org.postgresql.Driver")
            .save()
        )

    alert_rows = enriched.filter((col("volatility") >= 0.015) | (spark_abs(col("sentiment_score")) >= 2.0))
    (
        alert_rows.select(
            concat_ws("-", col("symbol"), col("window_start").cast("string")).alias("key"),
            to_json(struct("*")).alias("value"),
        )
        .write.format("kafka")
        .options(**kafka_options())
        .option("topic", ALERTS_TOPIC)
        .save()
    )

    enriched.foreachPartition(write_to_redis)


def main() -> None:
    spark = (
        SparkSession.builder.appName("real-time-crypto-intelligence-pipeline")
        .config("spark.sql.shuffle.partitions", env("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))

    trades = (
        read_topic(spark, TRADES_TOPIC)
        .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp("event_time"))
        .withWatermark("event_ts", "2 minutes")
    )

    news = (
        read_topic(spark, NEWS_TOPIC)
        .select(from_json(col("value").cast("string"), news_schema).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp("published_at"))
        .withColumn("sentiment_score", sentiment_udf(concat_ws(" ", col("title"), col("description"))))
        .withWatermark("event_ts", "10 minutes")
    )

    fred = (
        read_topic(spark, FRED_TOPIC)
        .select(from_json(col("value").cast("string"), fred_schema).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp("ingestion_time"))
        .withWatermark("event_ts", "1 day")
    )

    trade_windows = (
        trades.groupBy(window("event_ts", "1 minute"), "symbol")
        .agg(
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_sum("quantity").alias("total_quantity"),
            stddev("price").alias("price_stddev"),
            count("*").alias("trade_count"),
        )
        .withColumn("volatility", when(col("avg_price") > 0, col("price_stddev") / col("avg_price")).otherwise(lit(0.0)))
        .alias("trade_windows")
    )

    news_windows = (
        news.groupBy(window("event_ts", "5 minutes"))
        .agg(
            avg("sentiment_score").alias("sentiment_score"),
            count("*").alias("news_count"),
            first("title", ignorenulls=True).alias("sample_headline"),
        )
        .alias("news_windows")
    )

    fred_snapshot = (
        fred.groupBy(window("event_ts", "1 day"), "series_id")
        .agg(first("value", ignorenulls=True).alias("macro_value"))
        .groupBy("window")
        .agg(
            to_json(
                expr("map_from_entries(collect_list(named_struct('key', series_id, 'value', cast(macro_value as string))))")
            ).alias("macro_snapshot")
        )
        .alias("fred_snapshot")
    )

    enriched = (
        trade_windows.join(
            news_windows,
            expr(
                "trade_windows.window.start >= news_windows.window.start AND "
                "trade_windows.window.start < news_windows.window.end"
            ),
            "leftOuter",
        )
        .join(
            fred_snapshot,
            expr(
                "trade_windows.window.start >= fred_snapshot.window.start AND "
                "trade_windows.window.start < fred_snapshot.window.end"
            ),
            "leftOuter",
        )
        .select(
            col("trade_windows.window.start").alias("window_start"),
            col("trade_windows.window.end").alias("window_end"),
            "symbol",
            "avg_price",
            "min_price",
            "max_price",
            "total_quantity",
            "trade_count",
            "volatility",
            "sentiment_score",
            "news_count",
            "sample_headline",
            "macro_snapshot",
        )
    )

    query = (
        enriched.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    print(f"Started crypto intelligence stream at {datetime.now(timezone.utc).isoformat()}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
