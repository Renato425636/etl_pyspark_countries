# Pipelines

## Countries ETL

**Trigger:** Manual (`python pipeline.py`)  
**Schedule:** Sob demanda  
**Fonte:** REST Countries API (`https://restcountries.com/v3.1/all`)

### Etapas

| Etapa | Módulo | Descrição |
|---|---|---|
| Extract | `src/extract/countries_api.py` | GET para a API, salva JSON bruto com retry de erros HTTP |
| Transform | `src/transform/countries_transform.py` | Validação de schema + flatten de currencies/languages + limpeza |
| Load | `src/load/parquet_writer.py` | Escrita em Parquet (mode=overwrite) |

### Schema de Saída

| Coluna | Tipo | Descrição |
|---|---|---|
| `nome_comum` | string | Nome comum do país |
| `nome_oficial` | string | Nome oficial do país |
| `regiao` | string | Região geográfica |
| `sub_regiao` | string | Sub-região |
| `capital` | string | Capital principal |
| `populacao` | int | População total |
| `area` | double | Área em km² |
| `moeda_codigo` | string | Código ISO da moeda |
| `moeda_nome` | string | Nome da moeda |
| `idioma` | string | Idioma oficial |
