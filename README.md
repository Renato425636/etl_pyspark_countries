# ETL PySpark Countries

Pipeline ETL com PySpark que extrai dados da API REST Countries, normaliza JSON aninhado (currencies, languages, capital) e salva em Parquet.

## Arquitetura

Ver [docs/architecture.md](docs/architecture.md) — pipeline em camadas: extract → transform → load.

## Stack

- Python 3.9+ · PySpark · requests · PyYAML · pytest · ruff · black · MkDocs Material

## Setup rápido

```bash
pip install -e ".[dev]"
python pipeline.py
```

## Estrutura

```
src/{extract,transform,load,config}/
tests/{unit,integration}/
docs/
config.yaml
pipeline.py
```

## Testes

```bash
pytest tests/unit -q
```

## Documentação

[GitHub Pages](https://renato425636.github.io/etl_pyspark_countries/)
