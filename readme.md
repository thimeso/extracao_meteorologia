# Meteorological Data Ingestion 

Este repositório reúne experimentos de engenharia de dados voltados à **extração e padronização de dados meteorológicos e ambientais provenientes de fontes públicas do governo brasileiro**.

O objetivo principal deste projeto foi **avaliar a viabilidade técnica de ingestão automatizada de diferentes bases de dados climáticos**, com foco em alimentar pipelines analíticos voltados à análise ambiental e climática.

Essas extrações foram desenvolvidas no contexto de um projeto acadêmico vinculado ao curso de **Gestão da Informação da Universidade Federal do Paraná (UFPR)**, tendo como aplicação prática a estruturação de fluxos de dados para qualquer empresa que atue com inteligência climática, sustentabilidade e análise de riscos ambientais.

---

# Objetivo do Projeto

O projeto tem como objetivo principal investigar formas de:

- Automatizar a coleta de dados meteorológicos públicos
- Padronizar dados provenientes de diferentes fontes
- Estruturar pipelines de ingestão de dados ambientais
- Produzir datasets prontos para análise e modelagem
- Contribuir para a construção de um **lake de dados meteorológicos**

Esse processo é fundamental para organizações que trabalham com **dados climáticos, ESG e análise de risco ambiental**, onde a integração de múltiplas fontes de dados é um dos principais desafios informacionais.

---

# Fontes de Dados Investigadas

Durante o projeto foram analisadas diversas fontes de dados meteorológicos públicas, entre elas:

### INMET — Instituto Nacional de Meteorologia

- Dados históricos das estações meteorológicas
- Tabela de dados das estações (consulta online)

Fontes:

https://portal.inmet.gov.br/dadoshistoricos
https://tempo.inmet.gov.br/TabelaEstacoes


Scripts implementados:

- `inmet_dados_historicos.py`
- `inmet_tabela_estacoes.py`

---

### CEMADEN — Centro Nacional de Monitoramento e Alertas de Desastres Naturais

Dados relacionados a:

- pluviômetros
- sensores hidrológicos
- monitoramento de eventos extremos

Fonte:

https://mapainterativo.cemaden.gov.br/


Script implementado:

- `cemaden_radares.py`

---

# Resultados Obtidos

Durante o desenvolvimento foram implementadas rotinas para:

- download automatizado de datasets
- automação de interfaces web
- extração de tabelas HTML
- normalização de colunas
- geração de arquivos estruturados (CSV e Parquet)

Os dados extraídos foram padronizados em estruturas tabulares consistentes para posterior ingestão em pipelines de dados.

---

# Limitações Encontradas

Embora o objetivo inicial fosse realizar a **extração automatizada de diversas fontes governamentais**, foram encontradas limitações importantes relacionadas ao acesso aos dados.

Alguns dos principais obstáculos observados foram:

- bloqueios de automação em interfaces web
- uso de CAPTCHAs em páginas de download
- mudanças frequentes na estrutura dos portais
- ausência de APIs públicas oficiais para algumas bases
- inconsistências nos formatos de dados disponibilizados

Devido a essas restrições técnicas e de acesso impostas pelos próprios portais governamentais, **nem todas as fontes planejadas puderam ser extraídas de forma automatizada**.

Ainda assim, os scripts presentes neste repositório demonstram os métodos testados e documentam as estratégias utilizadas para contornar essas limitações sempre que possível.

---

# Tecnologias Utilizadas

As rotinas de ingestão foram desenvolvidas utilizando:

- Python
- Pandas
- Selenium
- PyArrow
- WebDriver Manager

Essas ferramentas foram utilizadas para construir fluxos de extração, transformação e armazenamento de dados.

---

# Saídas Geradas

Os scripts produzem arquivos estruturados em formatos adequados para pipelines de dados:

- CSV
- Parquet

Os datasets são normalizados com:

- nomes de colunas padronizados
- formatação consistente
- metadados de extração

---

# Contexto Acadêmico

Este projeto foi desenvolvido no contexto do estágio supervisionado do curso de **Gestão da Informação da Universidade Federal do Paraná**, com foco na melhoria dos fluxos informacionais e na integração de dados meteorológicos para suporte a análises ambientais e climáticas.

A proposta envolve a construção de pipelines de dados que permitam estruturar e integrar múltiplas fontes de informação, contribuindo para soluções analíticas relacionadas à sustentabilidade e ao monitoramento climático.

---

# Observação

Este repositório possui caráter **experimental e exploratório**, tendo como objetivo principal avaliar a viabilidade de ingestão automatizada de dados meteorológicos públicos.

Devido às limitações técnicas e de acesso impostas pelos portais governamentais, algumas extrações planejadas não puderam ser implementadas integralmente.

Mesmo assim, os scripts presentes documentam os métodos testados e podem servir como base para futuros desenvolvimentos ou adaptações caso as interfaces públicas evoluam para modelos mais acessíveis (como APIs abertas).

---

# Autor

Thiago Messias Soares  
Gestão da Informação — UFPR