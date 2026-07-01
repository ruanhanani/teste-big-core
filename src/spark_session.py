"""Criacao da SparkSession usada em todo o pipeline."""
from __future__ import annotations

from pyspark.sql import SparkSession

from .config import Config


def build_spark(config: Config) -> SparkSession:
    return (
        SparkSession.builder.appName(config.spark_app_name)
        .master(config.spark_master)
        .config("spark.driver.memory", config.spark_driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        # Escritas idempotentes: sobrescrever apenas as particoes afetadas.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
