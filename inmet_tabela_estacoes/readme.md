# INMET Table of Station Data Extraction

Script responsável pela extração de dados tabulares da interface web
"Tabela de Dados das Estações" do INMET, utilizando Selenium.

## Objetivo

Automatizar o processo de:

- abertura da interface de consulta
- seleção do intervalo de datas
- geração da tabela
- extração dos dados renderizados
- normalização das colunas
- exportação em CSV e Parquet

## Observação sobre a estação A001

O código `A001` presente na URL é apenas um exemplo de estação meteorológica.
O script pode ser adaptado para outras estações disponíveis no portal.

## Fonte

https://tempo.inmet.gov.br/TabelaEstacoes/A001

## Estrutura do fluxo

1. carregar página da estação
2. abrir painel lateral
3. preencher datas
4. gerar tabela
5. extrair dados renderizados
6. salvar arquivos finais

## Colunas finais

O arquivo gerado contém colunas normalizadas como:

- `temperatura_c_inst`
- `temperatura_c_max`
- `temperatura_c_min`
- `umidade_perc_inst`
- `pressao_hpa_inst`
- `vento_vel_ms`
- `radiacao_kj_m2`
- `chuva_mm`

## Dependências

Instalar com:

```bash
pip install -r requirements.txt