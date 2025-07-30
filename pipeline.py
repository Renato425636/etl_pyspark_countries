import logging
import sys
import json
import requests
import findspark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (col,explode_outer,element_at,coalesce,lit,struct)
from pyspark.sql.types import (IntegerType,DoubleType,StringType,StructType,StructField,MapType,ArrayType)


CONFIG = {
    "api_url": "https://restcountries.com/v3.1/all",
    "raw_data_path": "dados_brutos_paises.json",
    "processed_data_path": "dados_processados_paises.parquet",
    "log_level": "INFO",
    "app_name": "PipelineJSONPaisesRobusto",
    "spark_master": "local[*]",
    "spark_driver_memory": "2g"
}


def setup_logging():
    """Configura o logger para a aplicação."""
    logging.basicConfig(
        level=CONFIG["log_level"],
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    return logging.getLogger(CONFIG["app_name"])

logger = setup_logging()


def inicializar_spark() -> SparkSession:
    
    try:
        findspark.init()
        spark = SparkSession.builder \
            .appName(CONFIG["app_name"]) \
            .master(CONFIG["spark_master"]) \
            .config("spark.driver.memory", CONFIG["spark_driver_memory"]) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        logger.info("SparkSession inicializada com sucesso.")
        return spark
    except Exception as e:
        logger.critical(f"Falha ao inicializar SparkSession: {e}", exc_info=True)
        sys.exit(1)



def extrair_dados_api(url: str, output_path: str):

    try:
        logger.info(f"Iniciando extração da API: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status() 
        
        dados = response.json()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
        logger.info(f"Dados brutos salvos com sucesso em: {output_path}")
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro de status HTTP: {e.response.status_code} - {e.response.text}")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Erro de conexão com a API: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error("A requisição para a API expirou (timeout).")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro inesperado na requisição à API: {e}")
        raise

def validar_schema_bruto(df: DataFrame):

    logger.info("Iniciando validação de schema dos dados brutos.")
    required_fields = {
        "name": StructType([StructField("common", StringType(), True), StructField("official", StringType(), True)]),
        "currencies": MapType(StringType(), StructType([StructField("name", StringType(), True), StructField("symbol", StringType(), True)])),
        "languages": MapType(StringType(), StringType()),
        "capital": ArrayType(StringType()),
        "population": IntegerType(),
        "area": DoubleType(),
        "region": StringType(),
        "subregion": StringType()
    }
    
    df_schema = {field.name: field.dataType for field in df.schema.fields}

    for field, expected_type in required_fields.items():
        if field not in df_schema:
            raise TypeError(f"Campo obrigatório '{field}' não encontrado no schema de origem.")

        if not isinstance(df_schema[field], type(expected_type)):
             logger.warning(f"Tipo do campo '{field}' ({type(df_schema[field])}) é diferente do esperado ({type(expected_type)}). Continuando com a transformação.")
    
    logger.info("Validação de schema concluída com sucesso.")


def transformar_dados_paises(spark: SparkSession, input_path: str) -> DataFrame:

    try:
        logger.info("Iniciando a etapa de transformação.")
        df_raw = spark.read.option("multiLine", "True").json(input_path)
        
        validar_schema_bruto(df_raw)

        logger.info("Aplicando transformações para achatar a estrutura.")
        
        df_exploded = df_raw.withColumn("currencies_map", explode_outer(col("currencies"))) \
                            .withColumn("languages_map", explode_outer(col("languages")))
        
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
            coalesce(col("languages_map.value"), lit("N/A")).alias("idioma")
        ).distinct()


        count_total = df_final.count()
        count_valid_population = df_final.filter("populacao >= 0").count()
        if count_total != count_valid_population:
            logger.warning(f"Foram encontrados {count_total - count_valid_population} registros com população inválida.")

        logger.info("Transformação concluída.")
        return df_final

    except Exception as e:
        logger.error(f"Erro durante a transformação dos dados com PySpark: {e}", exc_info=True)
        raise

def carregar_dados_parquet(df: DataFrame, output_path: str):

    try:
        logger.info(f"Iniciando carregamento para o formato Parquet em: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        logger.info("Dados salvos em Parquet com sucesso.")
    except Exception as e:
        logger.error(f"Falha ao salvar o DataFrame em Parquet: {e}", exc_info=True)
        raise


def main():

    logger.info("--- INICIANDO PIPELINE DE PROCESSAMENTO DE DADOS DE PAÍSES ---")
    spark = None
    try:
        # Etapa 1: Extração
        extrair_dados_api(CONFIG["api_url"], CONFIG["raw_data_path"])
        
        # Inicializa Spark apenas se a extração for bem-sucedida
        spark = inicializar_spark()
        
        # Etapa 2: Transformação
        df_transformado = transformar_dados_paises(spark, CONFIG["raw_data_path"])
        
        logger.info("Schema final do DataFrame transformado:")
        df_transformado.printSchema()
        
        logger.info("Amostra dos 10 primeiros registros transformados:")
        df_transformado.show(10, truncate=False)
        
        # Etapa 3: Carregamento
        carregar_dados_parquet(df_transformado, CONFIG["processed_data_path"])
        
        logger.info("--- PIPELINE CONCLUÍDO COM SUCESSO ---")
        
    except Exception as e:
        logger.critical(f"--- FALHA NA EXECUÇÃO DO PIPELINE: {e} ---", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession finalizada.")


if __name__ == "__main__":
    main()
