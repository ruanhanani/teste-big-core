"""Camada GOLD — modelo analitico e metricas de negocio.

Consolida a tabela de viagens enriquecidas (fato central) e materializa as
metricas pedidas no enunciado. Cada tabela e gravada como Parquet e servida ao
dashboard via DuckDB. Escritas em modo overwrite garantem idempotencia.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .logging_conf import get_logger

logger = get_logger("gold")


def _write(df: DataFrame, cfg: Config, name: str, partition: str | None = None) -> DataFrame:
    writer = df.write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.parquet(str(cfg.gold_dir / name))
    logger.info("gold.%s -> %s linhas", name, df.count())
    return df


def _viagens_enriquecidas(spark: SparkSession, cfg: Config) -> DataFrame:
    viagens = spark.read.parquet(str(cfg.silver_dir / "viagens"))
    veiculos = spark.read.parquet(str(cfg.silver_dir / "veiculos"))
    motoristas = spark.read.parquet(str(cfg.silver_dir / "motoristas"))
    geocercas = spark.read.parquet(str(cfg.silver_dir / "geocercas"))
    posicoes_geo = spark.read.parquet(str(cfg.silver_dir / "posicoes_geo"))

    velocidade = posicoes_geo.groupBy("viagem_id").agg(
        F.round(F.avg("velocidade_kmh"), 1).alias("velocidade_media_kmh"),
        F.max("velocidade_kmh").alias("velocidade_max_kmh"),
        F.count("*").alias("qtd_posicoes"),
    )

    veic = veiculos.select(
        "veiculo_id",
        "placa",
        "marca",
        "modelo",
        F.col("tipo").alias("tipo_veiculo"),
        F.col("status").alias("status_veiculo"),
    )
    mot = motoristas.select(
        "motorista_id",
        F.col("nome").alias("nome_motorista"),
        "base_operacional",
        "categoria_cnh",
    )
    origem = geocercas.select(
        F.col("geocerca_id").alias("geocerca_origem_id"),
        F.col("nome").alias("origem_nome"),
        F.col("tipo").alias("origem_tipo"),
    )
    destino = geocercas.select(
        F.col("geocerca_id").alias("geocerca_destino_id"),
        F.col("nome").alias("destino_nome"),
        F.col("tipo").alias("destino_tipo"),
    )

    enr = (
        viagens.join(veic, "veiculo_id", "left")
        .join(mot, "motorista_id", "left")
        .join(origem, "geocerca_origem_id", "left")
        .join(destino, "geocerca_destino_id", "left")
        .join(velocidade, "viagem_id", "left")
        .withColumn("mes_referencia", F.date_format("data_inicio", "yyyy-MM"))
        .withColumn(
            "duracao_horas",
            F.round(
                (F.col("data_fim_real").cast("long") - F.col("data_inicio").cast("long"))
                / 3600.0,
                2,
            ),
        )
        .withColumn(
            "atraso_horas",
            F.round(
                (
                    F.col("data_fim_real").cast("long")
                    - F.col("data_fim_prevista").cast("long")
                )
                / 3600.0,
                2,
            ),
        )
        .withColumn("flag_atrasada", F.col("status") == "atrasada")
    )
    return _write(enr, cfg, "viagens_enriquecidas", partition="mes_referencia").cache()


def _metricas(cfg: Config, enr: DataFrame, spark: SparkSession) -> None:
    # 1. Viagens por mes e por status
    por_mes_status = enr.groupBy("mes_referencia", "status").count().withColumnRenamed(
        "count", "qtd_viagens"
    )
    _write(por_mes_status, cfg, "viagens_por_mes_status")

    # 2. Tempo medio de viagem por rota (origem -> destino)
    por_rota = (
        enr.filter(F.col("duracao_horas").isNotNull())
        .groupBy("geocerca_origem_id", "origem_nome", "geocerca_destino_id", "destino_nome")
        .agg(
            F.round(F.avg("duracao_horas"), 2).alias("tempo_medio_horas"),
            F.count("*").alias("qtd_viagens"),
        )
        .orderBy(F.desc("qtd_viagens"))
    )
    _write(por_rota, cfg, "tempo_medio_por_rota")

    # 3. Velocidade media por viagem
    vel_viagem = enr.select(
        "viagem_id", "mes_referencia", "velocidade_media_kmh", "velocidade_max_kmh"
    ).filter(F.col("velocidade_media_kmh").isNotNull())
    _write(vel_viagem, cfg, "velocidade_media_por_viagem")

    # 4. Taxa de atraso por mes
    taxa_atraso = enr.groupBy("mes_referencia").agg(
        F.count("*").alias("total_viagens"),
        F.sum(F.col("flag_atrasada").cast("int")).alias("viagens_atrasadas"),
    ).withColumn(
        "taxa_atraso",
        F.round(F.col("viagens_atrasadas") / F.col("total_viagens"), 4),
    )
    _write(taxa_atraso, cfg, "taxa_atraso_por_mes")

    # 5. Top 10 motoristas por viagens concluidas
    top_motoristas = (
        enr.filter(F.col("status") == "concluida")
        .groupBy("motorista_id", "nome_motorista")
        .agg(F.count("*").alias("viagens_concluidas"))
        .orderBy(F.desc("viagens_concluidas"))
        .limit(10)
    )
    _write(top_motoristas, cfg, "top10_motoristas")

    # 6. Utilizacao da frota por mes (veiculos ativos com viagem / total ativos)
    veiculos = spark.read.parquet(str(cfg.silver_dir / "veiculos"))
    ativos = veiculos.filter(F.col("status") == "ativo")
    total_ativos = ativos.count()
    utilizacao = (
        enr.join(ativos.select("veiculo_id"), "veiculo_id", "left_semi")
        .groupBy("mes_referencia")
        .agg(F.countDistinct("veiculo_id").alias("veiculos_utilizados"))
        .withColumn("total_veiculos_ativos", F.lit(total_ativos))
        .withColumn(
            "taxa_utilizacao",
            F.round(F.col("veiculos_utilizados") / F.lit(total_ativos), 4),
        )
    )
    _write(utilizacao, cfg, "utilizacao_frota_por_mes")

    # 7. Tempo medio parado em geocercas por tipo
    eventos = spark.read.parquet(str(cfg.silver_dir / "eventos_geocerca"))
    parado = (
        eventos.groupBy("tipo_geocerca")
        .agg(
            F.round(F.avg("duracao_segundos") / 60.0, 2).alias("tempo_medio_parado_min"),
            F.count("*").alias("qtd_visitas"),
        )
        .orderBy(F.desc("qtd_visitas"))
    )
    _write(parado, cfg, "tempo_medio_parado_por_tipo")


def run(spark: SparkSession, cfg: Config) -> None:
    cfg.gold_dir.mkdir(parents=True, exist_ok=True)
    enr = _viagens_enriquecidas(spark, cfg)
    _metricas(cfg, enr, spark)
    enr.unpersist()
