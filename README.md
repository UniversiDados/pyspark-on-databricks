# pyspark-on-databricks

| Conceito no Airflow | Conceito Equivalente no Databricks    | Descrição da Mudança                                                                                                                                                   |
|---------------------------------------|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DAG (Arquivo .py na pasta `dags`)     | Workflow (ou "Job")                    | No Databricks, você não escreve um arquivo Python para definir a estrutura do fluxo. Você cria um "Job" na interface gráfica (ou via API/Terraform) e adiciona tarefas. |
| Task (Operator, ex: PythonOperator)   | Task (Tarefa)                          | Cada etapa do seu fluxo se torna uma tarefa no Workflow do Databricks. Pode ser um Notebook, script Python, query SQL, pipeline DLT, etc.                              |
| Infraestrutura (Docker Compose)       | Cluster (Job Cluster)                  | Você para de gerenciar contêineres. Cada Workflow roda em um "Job Cluster" que liga, executa e desliga automaticamente. Paga-se só pelo tempo de execução.             |
| Scheduler                             | Scheduler do Job                       | O agendamento (ex: "rodar todo dia às 5h") é configurado visualmente ou via cron direto na interface do Databricks.                                                   |
| Dependências (`requirements.txt`)     | Bibliotecas do Cluster                 | As libs Python são instaladas no cluster do Databricks via `%pip` no notebook ou na configuração do cluster.                                                          |
| Connections & Variables               | Databricks Secrets & Parâmetros do Job | Em vez das conexões/variáveis do Airflow, usa-se Databricks Secrets para credenciais e parâmetros de Job para valores dinâmicos.                                      |


