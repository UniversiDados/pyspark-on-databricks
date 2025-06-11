# Visão Geral do Projeto: Migração de Pipeline ETL de Airflow para Databricks Workflows

Este projeto detalha a arquitetura e o plano de migração de um pipeline de ETL (Extração, Transformação e Carga) do seu ambiente atual, orquestrado pelo Apache Airflow em um setup Docker, para uma solução nativa na plataforma Databricks, utilizando Databricks Workflows.

O objetivo principal é substituir a orquestração via Airflow e o cluster Spark Standalone por componentes integrados do Databricks, como Jobs e Notebooks, para criar um pipeline mais unificado, eficiente e escalável.

---

## Arquitetura Atual (Airflow on Docker)

A infraestrutura inicial é baseada em um ambiente Docker local que utiliza o Apache Airflow para orquestrar as tarefas de um pipeline de dados.

### Componentes Principais:
* **Orquestrador:** Apache Airflow, executado via Docker Compose. 
* **Banco de Dados de Metadados:** PostgreSQL para o Airflow. 
* **Processamento de Dados:** Um cluster Spark Standalone, também conteinerizado, para executar as tarefas de processamento. 
* **Fluxo de Trabalho (DAG):** O pipeline é definido no arquivo `dag_paises_pipeline.py`. 
* **Persistência de Dados:** Os dados brutos, processados e enriquecidos são salvos em formato Delta Lake  em um volume Docker mapeado para o sistema de arquivos local (`./data/delta`). 

### Fluxo de Trabalho da DAG:
O pipeline é composto por três tarefas principais orquestradas pelo Airflow: 

1.  **`extract_data` (PythonOperator):**
    * **Descrição:** Extrai dados sobre países a partir da API REST Countries. 
    * **Script:** `scripts/01_extract_api_data.py`. 
    * **Saída:** Salva os dados brutos em formato JSON. 

2.  **`process_data` (SparkSubmitOperator):**
    * **Descrição:** Processa os dados JSON brutos, aplica validações, achata estruturas de dados e calcula a densidade populacional. 
    * **Script:** `scripts/02_process_countries_data.py`. 
    * **Saída:** Armazena os dados processados em uma tabela Delta. 

3.  **`load_data` (SparkSubmitOperator):**
    * **Descrição:** Gera dados econômicos simulados (PIB per capita, IDH) e os utiliza para enriquecer a tabela de países processados. 
    * **Scripts:** `scripts/03_create_economic_data.py` e `scripts/04_enrich_countries_data.py`. 
    * **Saída:** Salva a tabela final enriquecida em formato Delta. 

---

## Arquitetura Proposta (Databricks Workflows)

A nova arquitetura moverá todo o pipeline para a plataforma Databricks, aproveitando seus recursos nativos para simplificar a orquestração, o processamento e o gerenciamento de dados.

### Mapeamento de Conceitos: Airflow para Databricks
| Conceito Airflow | Conceito Databricks Workflows |
| :--- | :--- |
| DAG (Directed Acyclic Graph) | Job (com múltiplas tarefas) |
| Task | Task (Notebook, JAR, Python, SQL) |
| Operator | Comando em Notebook, JAR, Python, SQL |
| XComs | Widgets de Notebook, Delta Lake, DBFS |
| Connections | Credenciais de Databricks, Secrets |

### Componentes Principais:
* **Orquestrador:** Databricks Jobs, que substituirá o Airflow para gerenciar o fluxo de trabalho.
* **Execução de Tarefas:** Os scripts Python/PySpark serão convertidos em Databricks Notebooks, permitindo interatividade, versionamento e colaboração.
* **Processamento de Dados:** Clusters gerenciados pelo Databricks eliminarão a necessidade de um cluster Spark Standalone separado. 
* **Armazenamento de Dados:**
    * **DBFS (Databricks File System):** Será utilizado para armazenar os dados brutos extraídos da API. 
    * **Delta Lake:** Continuará sendo o formato principal para as tabelas de dados processados e enriquecidos.

### Novo Fluxo de Trabalho (Databricks Job):
O pipeline será reestruturado como um único Job no Databricks, contendo quatro tarefas distintas, cada uma implementada como um Notebook:

1.  **`extract_data` (Notebook Python):**
    * **Descrição:** Extrai dados da API e salva o JSON bruto no DBFS.
    * **Dependências:** Nenhuma. 

2.  **`process_data` (Notebook PySpark):**
    * **Descrição:** Lê o JSON bruto do DBFS, executa as transformações e salva como uma tabela Delta processada.
    * **Dependências:** `extract_data`. 

3.  **`create_economic_data` (Notebook PySpark):**
    * **Descrição:** Gera dados econômicos simulados e os salva como uma tabela Delta.
    * **Dependências:** Nenhuma (pode executar em paralelo com `process_data`). 

4.  **`enrich_countries_data` (Notebook PySpark):**
    * **Descrição:** Junta os dados de países processados com os dados econômicos simulados.
    * **Dependências:** `process_data` e `create_economic_data`. 

### Vantagens da Nova Arquitetura:
* **Otimização de Custos:** Capacidade de dimensionar e desligar clusters automaticamente ao final do job. 
* **Gerenciamento Simplificado:** Logging , monitoramento e alertas  são recursos nativos dos Databricks Jobs.
* **Controle de Versão:** Notebooks podem ser integrados diretamente com repositórios Git. 

---

## Próximos Passos para a Migração

O plano para implementar a nova arquitetura envolve as seguintes etapas:

1.  Adaptar os scripts Python e PySpark existentes para o formato de Databricks Notebooks. 
2.  Configurar um Databricks Job, definindo as quatro tarefas e suas respectivas dependências. 
3.  Executar testes no Job para validar o fluxo e a integridade dos dados gerados. 
4.  Documentar a implementação final e criar um guia de uso. 

| Conceito no Airflow | Conceito Equivalente no Databricks    | Descrição da Mudança                                                                                                                                                   |
|---------------------------------------|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DAG (Arquivo .py na pasta `dags`)     | Workflow (ou "Job")                    | No Databricks, você não escreve um arquivo Python para definir a estrutura do fluxo. Você cria um "Job" na interface gráfica (ou via API/Terraform) e adiciona tarefas. |
| Task (Operator, ex: PythonOperator)   | Task (Tarefa)                          | Cada etapa do seu fluxo se torna uma tarefa no Workflow do Databricks. Pode ser um Notebook, script Python, query SQL, pipeline DLT, etc.                              |
| Infraestrutura (Docker Compose)       | Cluster (Job Cluster)                  | Você para de gerenciar contêineres. Cada Workflow roda em um "Job Cluster" que liga, executa e desliga automaticamente. Paga-se só pelo tempo de execução.             |
| Scheduler                             | Scheduler do Job                       | O agendamento (ex: "rodar todo dia às 5h") é configurado visualmente ou via cron direto na interface do Databricks.                                                   |
| Dependências (`requirements.txt`)     | Bibliotecas do Cluster                 | As libs Python são instaladas no cluster do Databricks via `%pip` no notebook ou na configuração do cluster.                                                          |
| Connections & Variables               | Databricks Secrets & Parâmetros do Job | Em vez das conexões/variáveis do Airflow, usa-se Databricks Secrets para credenciais e parâmetros de Job para valores dinâmicos.                                      |


