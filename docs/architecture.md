# Arquitetura

## Estrutura de Camadas

```
src/
├── config/    # Carregamento de config.yaml
├── extract/   # Chamada à API REST Countries
├── transform/ # Flatten de JSON aninhado com PySpark
└── load/      # Escrita em Parquet
```

## Decisões de Design

| Decisão | Justificativa |
|---|---|
| `explode_outer` para currencies/languages | Mantém países sem moeda/idioma (não perde linhas) |
| `coalesce(..., lit("N/A"))` | Garante consistência — sem nulls no output |
| Schema validation antes do transform | Falha rápida se a API mudar sua estrutura |
| Config em YAML externo | Pipeline agnóstico a ambiente |

## Stack

| Componente | Tecnologia |
|---|---|
| Processamento | Apache Spark (PySpark) |
| Linguagem | Python 3.9+ |
| Extração | requests |
| Configuração | YAML |
| Testes | pytest |
| Linting | ruff + black |
| Docs | MkDocs Material |
