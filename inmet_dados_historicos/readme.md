# INMET Historical Data Ingestion

Script responsável pela ingestão de dados históricos anuais do INMET.

## Objetivo

Automatizar o processo de:

- download dos arquivos ZIP anuais
- leitura dos CSVs internos
- extração dos metadados das estações
- consolidação dos dados
- exportação em formato analítico

## Estrutura do pipeline

O script segue três camadas:

### RAW
Armazena o arquivo ZIP original baixado do portal do INMET.

### INT
Extrai e normaliza os arquivos CSV internos, convertendo os metadados
das oito primeiras linhas em colunas tabulares.

### EXP
Exporta os dados finais em um único arquivo Parquet por ano.

## Fonte de dados

https://portal.inmet.gov.br/dadoshistoricos

## Estrutura de saída

```text
data/
├── raw/
│   └── inmet/
├── int/
│   └── inmet/
├── exp/
│   └── inmet/
└── logs/
```

## Dependências

Instalar dependências com:

```bash
pip install -r requirements.txt