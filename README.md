# README para o Projeto de Análise de Dados com PySpark e BigQuery

### Introdução

Terceiro Projeto FIAP/2024


### Looker dashboard

https://lookerstudio.google.com/reporting/a6af6c92-d931-4aeb-942a-857bfb6450b2


### Iniciar Projeto

```bash
poetry new poetry-demo
source env/bin/activate
```

### Install all dependencies

```bash
poetry install
```

### Important files

- [x] README.md
- [x] .gitignore
- [x] pyproject.toml
- [x] poetry.lock
- [x] src/create_consolidated_table.py

`src/create_consolidated_table.py` is the main file that will be used to create the consolidated table.

## Descrição do Projeto
Este projeto realiza a análise e transformação de dados da PNAD COVID-19 de maio a julho de 2020, utilizando PySpark para processamento de dados em larga escala e BigQuery como destino para armazenamento e análise posterior. O objetivo é consolidar os dados mensais em um único conjunto de dados transformado, aplicando mapeamentos específicos e renomeando colunas para uma melhor compreensão e análise.

## Processo de Execução

### Configuração do Ambiente
- **Credenciais do Google Cloud**: Estabelece a autenticação para acessar o BigQuery através de um arquivo de credenciais JSON.
- **Sessão Spark com suporte ao BigQuery**: Inicializa a sessão do Spark configurando o ambiente para permitir a leitura e gravação de dados no BigQuery.

### Leitura de Dados
- Os dados são lidos de tabelas específicas do BigQuery referentes aos meses de maio a julho de 2020 da pesquisa PNAD COVID-19.

### Transformação de Dados
- **Seleção de Colunas Comuns**: Identifica e seleciona as colunas comuns entre os meses para garantir a consistência dos dados.
- **Renomeação de Colunas**: As colunas são renomeadas para termos mais descritivos, facilitando a compreensão dos dados.
- **Mapeamento de Valores**: Aplica transformações específicas para converter códigos em valores legíveis, como estados, gênero, escolaridade e a principal atividade do trabalho.

### Consolidação dos Dados
- Os dados dos três meses são unificados em um único DataFrame para análise consolidada.

### Carga de Dados
- O DataFrame transformado e consolidado é carregado no BigQuery para armazenamento e análises futuras.

## Uso

Para executar este projeto, você precisará de um ambiente Python configurado com PySpark e acesso ao Google Cloud BigQuery. Configure as credenciais do Google Cloud e atualize os nomes das tabelas do BigQuery conforme necessário.

## Estrutura do Código

- Configuração inicial para conexão com BigQuery e leitura dos dados.
- Transformação dos dados, incluindo renomeação e mapeamento de valores.
- União dos DataFrames mensais em um único conjunto para análise.
- Carregamento do DataFrame resultante no BigQuery.

Este projeto é uma ferramenta poderosa para transformar e analisar grandes volumes de dados, tirando proveito das capacidades do PySpark para processamento distribuído e do BigQuery para armazenamento e análise de dados em escala.