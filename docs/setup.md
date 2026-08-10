# Setup

## Pré-requisitos

- Python 3.9+
- Java 8 ou 11 (necessário para PySpark)
- Conexão com internet (para a API REST Countries)

## Instalação

```bash
git clone https://github.com/Renato425636/etl_pyspark_countries.git
cd etl_pyspark_countries
pip install -e ".[dev]"
```

## Execução

```bash
python pipeline.py
```

## Testes

```bash
pytest tests/unit -q
```

## Saídas

- `data/raw/paises.json` — dados brutos da API
- `data/processed/paises.parquet/` — dados transformados em Parquet
