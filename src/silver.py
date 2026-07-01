"""Camada SILVER — limpeza, padronizacao e integridade referencial.

Cada dataset bruto vira uma tabela confiavel:
  * tipos corrigidos (datas, numeros, booleanos);
  * strings/categoricos padronizados (trim + lower quando aplicavel);
  * nulos, duplicatas e valores absurdos tratados;
  * integridade referencial garantida (viagens sem veiculo/motorista sao
    removidas; posicoes fora do Brasil, zeradas ou com velocidade impossivel
    sao descartadas).
As regras de descarte sao logadas para rastreabilidade.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Config
from .logging_conf import get_logger
from .quality import cpf_valido_udf, placa_valida

logger = get_logger("silver")


def _read_bronze(spark: SparkSession, cfg: Config, name: str) -> DataFrame:
    return spark.read.parquet(str(cfg.bronze_dir / name))


def _write(df: DataFrame, cfg: Config, name: str) -> DataFrame:
    df.write.mode("overwrite").parquet(str(cfg.silver_dir / name))
    logger.info("silver.%s -> %s linhas", name, df.count())
    return df


def _write_rejeitados(df: DataFrame, cfg: Config, name: str) -> None:
    """Quarentena: registros descartados ficam rastreaveis, com o motivo."""
    df = df.withColumn("camada_origem", F.lit(name))
    df.write.mode("overwrite").parquet(str(cfg.rejeitados_dir / name))
    logger.info("rejeitados.%s -> %s registros", name, df.count())


def _clean_veiculos(spark: SparkSession, cfg: Config) -> DataFrame:
    df = _read_bronze(spark, cfg, "veiculos")
    df = (
        df.filter(F.col("veiculo_id").isNotNull())
        .dropDuplicates(["veiculo_id"])
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("tipo", F.trim("tipo"))
        .withColumn("placa", F.upper(F.trim("placa")))
        .withColumn("placa_valida", placa_valida("placa"))
        .withColumn("ano_fabricacao", F.col("ano_fabricacao").cast("int"))
        .withColumn("capacidade_kg", F.col("capacidade_kg").cast("int"))
        .withColumn("capacidade_paletes", F.col("capacidade_paletes").cast("int"))
        .withColumn("km_atual", F.col("km_atual").cast("int"))
        .withColumn("data_ultima_revisao", F.to_date("data_ultima_revisao"))
    )
    return _write(df, cfg, "veiculos")


def _clean_motoristas(spark: SparkSession, cfg: Config) -> DataFrame:
    df = _read_bronze(spark, cfg, "motoristas")
    df = df.filter(F.col("motorista_id").isNotNull())
    df = df.withColumn("nome", F.trim("nome")).withColumn(
        "nome", F.when(F.col("nome") == "", None).otherwise(F.col("nome"))
    )
    # Dedup deterministica: preferimos o registro com nome preenchido.
    janela = Window.partitionBy("motorista_id").orderBy(
        F.col("nome").asc_nulls_last(), F.col("cpf").asc_nulls_last()
    )
    df = (
        df.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("categoria_cnh", F.upper(F.trim("categoria_cnh")))
        .withColumn("cpf_valido", cpf_valido_udf(F.col("cpf")))
        .withColumn("validade_cnh", F.to_date("validade_cnh"))
        .withColumn("data_admissao", F.to_date("data_admissao"))
    )
    return _write(df, cfg, "motoristas")


def _clean_viagens(
    spark: SparkSession, cfg: Config, veiculos: DataFrame, motoristas: DataFrame
) -> DataFrame:
    df = _read_bronze(spark, cfg, "viagens")
    bruto = df.count()

    df = (
        df.filter(F.col("viagem_id").isNotNull())
        .dropDuplicates(["viagem_id"])
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("data_inicio", F.to_timestamp("data_inicio"))
        .withColumn("data_fim_prevista", F.to_timestamp("data_fim_prevista"))
        .withColumn("data_fim_real", F.to_timestamp("data_fim_real"))
        .withColumn("distancia_km", F.col("distancia_km").cast("int"))
        .withColumn("peso_carga_kg", F.col("peso_carga_kg").cast("int"))
    )

    # Flags de existencia para integridade referencial.
    vids = veiculos.select("veiculo_id").distinct().withColumn("_v", F.lit(True))
    mids = motoristas.select("motorista_id").distinct().withColumn("_m", F.lit(True))
    df = df.join(vids, "veiculo_id", "left").join(mids, "motorista_id", "left")

    # Motivo de rejeicao (prioridade: data > veiculo > motorista).
    motivo = (
        F.when(F.col("data_inicio").isNull(), "data_inicio ausente")
        .when(F.col("_v").isNull(), "veiculo_id inexistente")
        .when(F.col("_m").isNull(), "motorista_id inexistente")
    )
    df = df.withColumn("motivo_rejeicao", motivo).drop("_v", "_m")

    rejeitados = df.filter(F.col("motivo_rejeicao").isNotNull())
    _write_rejeitados(
        rejeitados.select(
            "viagem_id", "veiculo_id", "motorista_id", "status", "data_inicio",
            "motivo_rejeicao",
        ),
        cfg,
        "viagens",
    )

    validas = (
        df.filter(F.col("motivo_rejeicao").isNull())
        .drop("motivo_rejeicao")
        # distancia negativa e fisicamente impossivel -> nula.
        .withColumn(
            "distancia_km",
            F.when(F.col("distancia_km") < 0, None).otherwise(F.col("distancia_km")),
        )
    )
    logger.info("viagens: %s brutas -> %s validas", bruto, validas.count())
    return _write(validas, cfg, "viagens")


def _clean_posicoes(spark: SparkSession, cfg: Config, viagens: DataFrame) -> DataFrame:
    df = _read_bronze(spark, cfg, "posicoes")
    bruto = df.count()
    bbox = cfg.brasil_bbox

    df = (
        df.dropDuplicates(["posicao_id"])
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("velocidade_kmh", F.col("velocidade_kmh").cast("int"))
    )

    # Marca existencia de viagem valida (posicoes de viagens rejeitadas ficam orfas).
    vv = viagens.select("viagem_id").distinct().withColumn("_viag", F.lit(True))
    df = df.join(vv, "viagem_id", "left")

    fora_do_brasil = ~(
        F.col("latitude").between(bbox.lat_min, bbox.lat_max)
        & F.col("longitude").between(bbox.lon_min, bbox.lon_max)
    )
    motivo = (
        F.when(F.col("timestamp").isNull(), "timestamp ausente")
        .when((F.col("latitude") == 0) | (F.col("longitude") == 0), "coordenada zerada")
        .when(fora_do_brasil, "fora do Brasil")
        .when(~F.col("velocidade_kmh").between(0, cfg.velocidade_max_kmh), "velocidade invalida")
        .when(F.col("_viag").isNull(), "viagem inexistente")
    )
    df = df.withColumn("motivo_rejeicao", motivo).drop("_viag")

    rejeitados = df.filter(F.col("motivo_rejeicao").isNotNull())
    _write_rejeitados(
        rejeitados.select(
            "posicao_id", "viagem_id", "veiculo_id", "latitude", "longitude",
            "timestamp", "velocidade_kmh", "motivo_rejeicao",
        ),
        cfg,
        "posicoes",
    )

    validas = df.filter(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")
    logger.info("posicoes: %s brutas -> %s validas", bruto, validas.count())
    return _write(validas, cfg, "posicoes")


def _clean_geocercas(spark: SparkSession, cfg: Config) -> DataFrame:
    df = _read_bronze(spark, cfg, "geocercas")
    df = (
        df.filter(F.col("geocerca_id").isNotNull() & F.col("geometry_wkt").isNotNull())
        .dropDuplicates(["geocerca_id"])
        .withColumn("tipo", F.lower(F.trim("tipo")))
        .withColumn("raio_km", F.col("raio_km").cast("double"))
    )
    return _write(df, cfg, "geocercas")


def run(spark: SparkSession, cfg: Config) -> None:
    cfg.silver_dir.mkdir(parents=True, exist_ok=True)
    cfg.rejeitados_dir.mkdir(parents=True, exist_ok=True)
    veiculos = _clean_veiculos(spark, cfg)
    motoristas = _clean_motoristas(spark, cfg)
    _clean_geocercas(spark, cfg)
    viagens = _clean_viagens(spark, cfg, veiculos, motoristas)
    _clean_posicoes(spark, cfg, viagens)
