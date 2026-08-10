import json
import logging
import os

import requests

logger = logging.getLogger(__name__)


def fetch_countries(url: str, output_path: str, timeout: int = 30) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    logger.info(f"Iniciando extração da API: {url}")
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=4)
        logger.info(f"Dados brutos salvos em: {output_path}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP {e.response.status_code}: {e.response.text}")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Erro de conexão com a API: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error("Requisição para a API expirou (timeout).")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro inesperado na requisição: {e}")
        raise
