import logging
import sys

import findspark
from pyspark.sql import SparkSession

from src.config.settings import load_config
from src.extract.countries_api import fetch_countries
from src.load.parquet_writer import save_parquet
from src.transform.countries_transform import transform_countries


def run(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)
    logging.basicConfig(
        level=config["log_level"],
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    logger = logging.getLogger(config["pipeline_name"])

    logger.info("--- INICIANDO PIPELINE DE PROCESSAMENTO DE DADOS DE PAÍSES ---")
    spark = None
    try:
        api_cfg = config["api"]
        data_cfg = config["data"]
        spark_cfg = config["spark"]

        fetch_countries(api_cfg["url"], data_cfg["raw_data_path"], api_cfg.get("timeout", 30))

        try:
            findspark.init()
        except Exception:
            pass

        spark = (
            SparkSession.builder
            .appName(spark_cfg["app_name"])
            .master(spark_cfg["master"])
            .config("spark.driver.memory", spark_cfg.get("driver_memory", "2g"))
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)

        df_transformed = transform_countries(spark, data_cfg["raw_data_path"])
        df_transformed.printSchema()
        df_transformed.show(10, truncate=False)

        save_parquet(df_transformed, data_cfg["processed_data_path"])

        logger.info("--- PIPELINE CONCLUÍDO COM SUCESSO ---")
    except Exception as e:
        logger.critical(f"--- FALHA NA EXECUÇÃO DO PIPELINE: {e} ---", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession finalizada.")


if __name__ == "__main__":
    run()
