# ETL PySpark Countries

Pipeline ETL com PySpark para extrair dados da API REST Countries, transformar JSON aninhado em formato tabular e salvar em Parquet.

## Visão Geral do Pipeline

```mermaid
flowchart LR
    A[API REST Countries] -->|GET /v3.1/all| B[Salva JSON bruto]
    B --> C[SparkSession]
    C --> D[Validação de Schema]
    D --> E[Flatten: explode currencies + languages]
    E --> F[Limpeza: coalesce + cast]
    F --> G[Salva Parquet]
```

## Links

- [Setup](setup.md)
- [Arquitetura](architecture.md)
- [Pipelines](pipelines.md)
