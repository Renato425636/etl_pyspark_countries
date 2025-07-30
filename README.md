# Pipeline de ETL com PySpark para Dados JSON

Este repositório contém um pipeline de ETL (Extração, Transformação e Carregamento) robusto, construído em Python e PySpark, projetado para processar dados semiestruturados em formato JSON. O projeto serve como um exemplo prático de como ingerir dados de uma API pública, normalizar estruturas complexas e aninhadas, e salvar o resultado em um formato otimizado para análise de dados.

## 🎯 Objetivo do Projeto

O objetivo principal é demonstrar a construção de um pipeline de dados resiliente e eficiente, capaz de lidar com desafios comuns em engenharia de dados, como:

  * **Ingestão de dados via API:** Conectar-se a um endpoint web para extrair dados.
  * **Processamento de JSON aninhado:** Lidar com objetos, arrays e mapas dentro de uma mesma estrutura de dados.
  * **Normalização e Limpeza:** Transformar dados brutos e complexos em um formato tabular (colunar) limpo e pronto para uso.
  * **Tolerância a falhas:** Garantir que o pipeline lide de forma graciosa com dados ausentes ou malformados.
  * **Boas práticas de engenharia de software:** Utilizar logging, modularização e configuração para criar um código legível e de fácil manutenção.

Para este projeto, utilizamos a API pública **[REST Countries](https://restcountries.com/)**, que fornece informações detalhadas sobre os países do mundo em um formato JSON rico e complexo.

## ✨ Principais Funcionalidades

  * **Extração de API:** Conecta-se à API REST Countries e baixa os dados de todos os países.
  * **Tratamento de Erros de Rede:** O pipeline é capaz de lidar com erros HTTP e de conexão durante a extração.
  * **Validação de Schema:** Antes do processamento, o script valida se os campos essenciais existem nos dados de origem, garantindo que o pipeline falhe rapidamente em caso de mudanças na estrutura da API.
  * **Transformação Robusta com `explode_outer`:** Utiliza `explode_outer` para achatar mapas e arrays, garantindo que nenhum registro seja perdido caso um país não tenha informações de moeda ou idioma.
  * **Limpeza e Padronização com `coalesce`:** Campos nulos (`null`) resultantes da transformação são substituídos por valores padrão (ex: "N/A" ou 0), garantindo a consistência dos dados.
  * **Tipagem de Dados:** Realiza a conversão explícita de colunas para os tipos de dados corretos (Integer, Double), otimizando o armazenamento e a performance de consultas.
  * **Logging Estruturado:** Registra todas as etapas importantes, sucessos e erros da execução, facilitando o monitoramento e a depuração.
  * **Saída em Formato Otimizado:** Salva o DataFrame final no formato **Parquet**, que é altamente eficiente para workloads analíticos em ecossistemas de Big Data.

## ⚙️ Tecnologias Utilizadas

  * **Python:** Linguagem principal para orquestração do pipeline.
  * **Apache Spark (PySpark):** Ferramenta de processamento de dados distribuído usada para a transformação em larga escala.
  * **Requests:** Biblioteca Python para realizar as chamadas à API HTTP.
  * **Findspark:** Utilitário para facilitar a localização da instalação do Spark em ambientes locais.

## 🚀 Como Executar o Pipeline

Siga os passos abaixo para executar o projeto em seu ambiente local.

### 1\. Pré-requisitos

  * **Python 3.9 ou superior.**
  * **Java Development Kit (JDK) 8 ou 11**, que é uma dependência do Apache Spark.
  * Clone este repositório:
    ```bash
    git clone https://github.com/seu-usuario/nome-do-repositorio.git
    cd nome-do-repositorio
    ```
  * Instale as bibliotecas Python necessárias:
    ```bash
    pip install pyspark findspark requests
    ```

### 2\. Execução

Com todos os pré-requisitos instalados, execute o script principal do pipeline:

```bash
python pipeline.py # ou o nome que você deu ao arquivo .py
```

### 3\. Verificar a Saída

Após a execução bem-sucedida, os seguintes artefatos serão criados no diretório do projeto:

  * 📄 **`dados_brutos_paises.json`**: Um arquivo JSON contendo os dados brutos e não modificados, exatamente como vieram da API.
  * 📁 **`dados_processados_paises.parquet/`**: Um diretório contendo os dados processados e limpos no formato Parquet. Este diretório pode ser lido por qualquer ferramenta de análise compatível com Parquet (Spark, Pandas, DuckDB, etc.).

## 📦 Arquitetura do Pipeline

O pipeline segue as três etapas clássicas do processo de ETL:

### 1\. Extração (Extract)

  * A função `extrair_dados_api` é chamada.
  * Ela envia uma requisição `GET` para a URL da API `restcountries.com/v3.1/all`.
  * O JSON de resposta é salvo localmente no arquivo `dados_brutos_paises.json`.
  * Tratamentos de erro verificam se a requisição foi bem-sucedida.

### 2\. Transformação (Transform)

  * Uma `SparkSession` é iniciada.
  * O arquivo `dados_brutos_paises.json` é carregado em um DataFrame Spark. O Spark infere o schema aninhado automaticamente.
  * **Validação de Schema:** A estrutura do DataFrame é verificada para garantir que os campos-chave (`name`, `currencies`, etc.) estão presentes.
  * **Achatamento (Flattening):**
      * `explode_outer(col("currencies"))`: Transforma o mapa de moedas. Para cada país, cria uma nova linha para cada moeda. Se um país não tiver moeda, a linha é mantida com valores nulos.
      * `explode_outer(col("languages"))`: O mesmo processo é aplicado para os idiomas.
      * `col("name.common")`: Campos dentro de objetos aninhados são acessados diretamente usando a notação de ponto.
      * `element_at(col("capital"), 1)`: O primeiro elemento do array de capitais é extraído.
  * **Limpeza e Conversão:**
      * `coalesce(col(...), lit(...))`: Garante que qualquer valor nulo seja substituído por um valor padrão.
      * `.cast(...)`: As colunas `population` e `area` são convertidas para tipos numéricos.
  * O resultado é um DataFrame limpo, tabular e com um schema bem definido.

### 3\. Carregamento (Load)

  * A função `carregar_dados_parquet` é executada.
  * O DataFrame transformado é salvo no diretório `dados_processados_paises.parquet/` usando o modo `overwrite`, que substitui os dados de execuções anteriores.

## 📈 Melhorias Futuras

Este projeto é uma base sólida que pode ser estendida com funcionalidades mais avançadas, como:

  * **Orquestração:** Integrar o pipeline com uma ferramenta como **Apache Airflow** ou **Mage** para agendar e monitorar execuções de forma automática.
  * **Containerização:** Empacotar a aplicação e suas dependências em um container **Docker** para garantir portabilidade e consistência entre ambientes.
  * **Testes:** Implementar testes unitários e de integração com `pytest` para validar a lógica de transformação e garantir a qualidade dos dados.
  * **Gerenciamento de Configuração:** Mover as configurações (URLs, caminhos, etc.) para arquivos externos, como `.yaml` ou `.env`.
  * **Schema Registry:** Utilizar um Schema Registry para gerenciar e validar a evolução do schema dos dados ao longo do tempo.

-----
