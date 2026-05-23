from data_processing import process_crypto_topic
from spark_session import create_spark_session
from table_creation import create_tables


if __name__ == "__main__":
    spark = create_spark_session()

    # Ensure gold namespace exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    create_tables(spark)

    process_crypto_topic(spark, kafka_topic="crypto_ticks").awaitTermination()
