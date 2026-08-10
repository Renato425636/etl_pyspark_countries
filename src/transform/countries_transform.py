import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, element_at, explode_outer, lit
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = {
    "name": StructType([
        StructField("common", StringType(), True),
        StructField("official", StringType(), True),
    ]),
    "currencies": MapType(StringType(), StructType([
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True),
    ])),
    "languages": MapType(StringType(), StringType()),
    "capital": ArrayType(StringType()),
    "population": IntegerType(),
    "area": DoubleType(),
    "region": StringType(),
    "subregion": StringType(),
}


def validate_raw_schema(df: DataFrame) -> None:
    logger.info("Validando schema dos dados brutos.")
    df_schema = {field.name: field.dataType for field in df.schema.fields}
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in df_schema:
            raise TypeError(f"Campo obrigatório '{field}' não encontrado no schema.")
        if not isinstance(df_schema[field], type(expected_type)):
            logger.warning(
                f"Tipo do campo '{field}' ({type(df_schema[field])}) "
                f"diferente do esperado ({type(expected_type)}). Continuando."
            )
    logger.info("Validação de schema concluída.")


def transform_countries(spark: SparkSession, input_path: str) -> DataFrame:
    logger.info("Iniciando transformação dos dados.")
    try:
        df_raw = spark.read.option("multiLine", "True").json(input_path)
        validate_raw_schema(df_raw)

        df_exploded = (
            df_raw
            .withColumn("currencies_map", explode_outer(col("currencies")))
            .withColumn("languages_map", explode_outer(col("languages")))
        )

        df_final = df_exploded.select(
            coalesce(col("name.common"), lit("N/A")).alias("nome_comum"),
            coalesce(col("name.official"), lit("N/A")).alias("nome_oficial"),
            coalesce(col("region"), lit("N/A")).alias("regiao"),
            coalesce(col("subregion"), lit("N/A")).alias("sub_regiao"),
            coalesce(element_at(col("capital"), 1), lit("N/A")).alias("capital"),
            coalesce(col("population").cast(IntegerType()), lit(0)).alias("populacao"),
            coalesce(col("area").cast(DoubleType()), lit(0.0)).alias("area"),
            coalesce(col("currencies_map.key"), lit("N/A")).alias("moeda_codigo"),
            coalesce(col("currencies_map.value.name"), lit("N/A")).alias("moeda_nome"),
            coalesce(col("languages_map.value"), lit("N/A")).alias("idioma"),
        ).distinct()

        count_total = df_final.count()
        invalid_pop = df_final.filter("populacao < 0").count()
        if invalid_pop > 0:
            logger.warning(f"{invalid_pop} registros com população inválida.")
        logger.info(f"Transformação concluída: {count_total} registros.")
        return df_final
    except Exception as e:
        logger.error(f"Erro durante a transformação: {e}", exc_info=True)
        raise
