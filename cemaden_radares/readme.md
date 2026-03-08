# CEMADEN Data Extraction

> [!WARNING]
> Durante o desenvolvimento do código foi implementado o recurso de CAPTCHA na extração dos dados, impossibilitando a extração dos mesmos.
> Decidiu-se manter o código, pois apesar de não conseguir baixar os dados sem autenticação, ele é funcional ao seu propoósito. 

Script responsável pela ingestão de dados pluviométricos disponibilizados
pelo Centro Nacional de Monitoramento e Alertas de Desastres Naturais (CEMADEN).

## Objetivo

Automatizar o processo de:

- download de arquivos de dados
- extração de arquivos compactados
- leitura e consolidação de CSVs
- normalização de colunas
- geração de arquivos estruturados para análise

## Estrutura

```text
cemaden/
│
├─ cemaden_extract.py
├─ requirements.txt
└─ README.md
```

## Saída

Os dados são exportados em:

- CSV
- Parquet

## Dependências

Instalar dependências com:

```bash
pip install -r requirements.txt