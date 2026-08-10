import logging
import os

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def save_parquet(df: DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    logger.info(f"Salvando dados em Parquet: {output_path}")
    try:
        df.write.mode("overwrite").parquet(output_path)
        logger.info("Dados salvos em Parquet com sucesso.")
    except Exception as e:
        logger.error(f"Falha ao salvar Parquet: {e}", exc_info=True)
        raise
