📊 ETL – Indicadores do Programa Brasil Saúde 360
🧩 Visão Geral
Pipeline de ETL (Extract, Transform, Load) desenvolvido em Python para processamento, padronização e consolidação de planilhas de indicadores do Programa Brasil Saúde 360.
O projeto foi estruturado com foco em:
Reprodutibilidade
Separação entre dados e código
Modularização do pipeline
Versionamento adequado
Boas práticas de engenharia de dados

🎯 Objetivos Técnicos
Automatizar a ingestão de planilhas heterogêneas
Padronizar esquemas de dados
Aplicar regras de transformação e validação
Gerar datasets consolidados para análise
Garantir rastreabilidade e organização do pipeline

🏗 Arquitetura do Projeto
ETL/
│
├── src/
│   ├── pipeline_m1.py      # Orquestração do pipeline
│   ├── df_m1.py            # Transformações específicas do módulo
│   └── utils.py            # Funções utilitárias e helpers
│
├── notebooks/              # Exploração e validação de dados
│
├── Dados/                  # Dados brutos (não versionados)
├── Resultados/             # Saídas processadas
├── models/                 # Artefatos e objetos serializados
│
├── .gitignore
└── README.md

⚙️ Stack Tecnológica
Python 3.x
Pandas
NumPy
Jupyter Notebook
Git (controle de versão)