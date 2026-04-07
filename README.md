# Data Projects Portfolio

## English

### About This Repository

I am a data engineer with experience building production data platforms. This repository is where I publish portfolio projects that demonstrate how I think about architecture, not just code that works.

The projects here are adapted from real business scenarios I have worked on, rebuilt with public or synthetic data for public sharing. The goal is to show the kind of decisions I make when designing data systems — what tools to use and why, how to separate concerns across layers, and how to write code that can actually be maintained and extended.

### What I Built

The main project in this repository is a **data platform built from scratch**, running fully on Docker and designed around the medallion architecture (Bronze → Silver → Gold).

The stack includes:

| Component | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.9 | Schedules and monitors the pipeline |
| Distributed processing | Apache Spark 3.5 | Joins and transforms raw data at scale |
| Analytical processing | Polars | Fast single-node aggregations on the silver layer |
| Object storage | MinIO | Local S3-compatible storage for all layers |
| Table format | Delta Lake 3.1 | ACID transactions and schema enforcement on silver |
| Containerization | Docker Compose | Full local platform, reproducible with one command |

The pipeline ingests the public [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle, processes it through three layers, and produces a price elasticity regression output at the gold layer.

### Architecture

The codebase is structured as a proper Python package (`data_projects_portfolio`, installable via `pip install -e .`) with two top-level concerns:

```
src/data_projects_portfolio/
├── infrastructure/              # Technology names — generic, reusable I/O clients
│   ├── delta_client.py          # SparkSession factory + Delta Lake read/write
│   ├── minio_client.py          # boto3 wrapper for MinIO uploads/downloads
│   ├── polars_client.py         # Reads Delta tables from MinIO using Polars
│   ├── spark_client.py          # Reads raw CSVs from MinIO — I/O only, no business logic
│   └── s3_client.py             # Uploads CSV output to MinIO/S3
└── domain/pricing/              # Business names — pure logic, zero I/O
    ├── models.py                # Dataclasses: ElasticityResult, RegressionFit, SeasonalKey
    ├── master_dataset.py        # Pure function: joins/filters/aggregates 5 Olist DataFrames
    └── elasticity.py            # Pure functions: regression, seasonality, tiers, imputation
dags/
└── olist_medallion.py           # The only Airflow-specific file in the entire project
```

Each DAG task follows the **I/O sandwich** pattern:

```python
@task
def silver() -> None:
    # READ — infrastructure client fetches raw data
    orders = SparkClient(spark).read_csv(bucket, "olist_orders_dataset.csv")
    ...
    # TRANSFORM — pure domain function, no I/O, fully unit-testable
    master = build_master_dataset(orders, items, customers, products, translation,
                                   start_date=start_date, end_date=end_date)
    # WRITE — infrastructure client persists the result
    DeltaClient(spark).write(master, silver_path)
```

This separation means the domain layer has **zero I/O** — it can be unit tested without Docker, without MinIO, and without a running Spark cluster.

### Engineering Decisions Worth Noting

**Why Spark for Bronze → Silver?**
The raw Olist dataset spans five CSVs with ~100k orders that need to be joined, filtered, and aggregated. Spark is the right tool for distributed joins at this scale, and it mirrors the kind of cost-aware decision I make in production: use the distributed engine where the data justifies it.

**Why Polars for Silver → Gold?**
After the Spark aggregation, the silver layer is medium-sized and single-node friendly. Polars is significantly faster than Pandas for the analytical operations at the gold layer and avoids the overhead of spinning up a Spark job for work that does not need it.

**Why does the domain layer have zero I/O?**
`domain/` contains pure Python functions with no database calls, no file reads, no Spark context. If a regression function is wrong, you find out in milliseconds with `pytest`, not after a 10-minute Spark job.

**Why is `dags/olist_medallion.py` the only Airflow file?**
If I need to swap Airflow for Prefect or Dagster tomorrow, only that file changes. The domain and infrastructure layers have no orchestrator dependency.

**Why `infrastructure/` instead of `adapters/`?**
`adapters/` implies Hexagonal Architecture ports-and-adapters, which comes with specific conventions. This codebase uses the pattern's intent (separate I/O from logic) without claiming the full pattern. `infrastructure/` says exactly what it is.

### Running This Project

```bash
make init       # copies .env.example → .env, creates needed directories
# edit .env with your Kaggle credentials
make up         # builds images and starts all containers
make logs       # follow container logs
```

UIs once running:
- Airflow: `localhost:8080` (admin / admin)
- MinIO: `localhost:9001` (minioadmin / minioadmin123)
- Spark Master: `localhost:8081`

```bash
make test-unit  # run domain tests — no Docker required
make test-int   # run integration tests — requires running containers
```

### VPN / Corporate Network Warning

If you are running this behind a corporate VPN with SSL inspection enabled, two things will fail:

**1. Docker build — Spark JARs**

The Spark image previously downloaded four JARs from Maven Central during build. The Dockerfile has already been updated to use `COPY` instead — the JARs are pre-downloaded and committed to `docker/spark/jars/`. No network access is needed for the Spark image build.

**2. DAG runtime — Kaggle download**

The bronze task downloads the Olist dataset from Kaggle via `kagglehub`. If your VPN intercepts HTTPS, this will fail with an SSL certificate error. Run this project on a machine without SSL-intercepting VPN, or inject your corporate CA certificate into the Docker images.

### Work In Progress

This portfolio is being actively built. The platform described above is functional, but I am continuing to add projects, improve test coverage, and document engineering decisions more thoroughly. New pipelines and use cases will be added over time.

---

## Português

### Sobre Este Repositório

Sou engenheiro de dados com experiência na construção de plataformas de dados em produção. Este repositório é onde publico projetos de portfólio que demonstram como penso sobre arquitetura, não apenas código que funciona.

Os projetos aqui são adaptados de cenários reais de negócio em que trabalhei, reconstruídos com dados públicos ou sintéticos para compartilhamento público. O objetivo é mostrar o tipo de decisão que tomo ao projetar sistemas de dados — quais ferramentas usar e por quê, como separar responsabilidades entre camadas e como escrever código que possa ser mantido e evoluído de verdade.

### O Que Eu Construí

O projeto principal neste repositório é uma **plataforma de dados construída do zero**, rodando completamente em Docker e projetada em torno da arquitetura medallion (Bronze → Silver → Gold).

O stack inclui:

| Componente | Tecnologia | Propósito |
|---|---|---|
| Orquestração | Apache Airflow 2.9 | Agenda e monitora o pipeline |
| Processamento distribuído | Apache Spark 3.5 | Joins e transformações em escala |
| Processamento analítico | Polars | Agregações rápidas em nó único na camada silver |
| Armazenamento de objetos | MinIO | Storage local compatível com S3 para todas as camadas |
| Formato de tabela | Delta Lake 3.1 | Transações ACID e validação de schema no silver |
| Containerização | Docker Compose | Plataforma local completa, reproduzível com um comando |

O pipeline ingere o dataset público [Olist Brazilian e-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) do Kaggle, processa em três camadas e produz uma saída de regressão de elasticidade de preço na camada gold.

### Arquitetura

O codebase é estruturado como um pacote Python oficial (`data_projects_portfolio`, instalável via `pip install -e .`) com duas responsabilidades principais:

```
src/data_projects_portfolio/
├── infrastructure/              # Nomes de tecnologia — clients genéricos de I/O
│   ├── delta_client.py          # SparkSession factory + leitura/escrita Delta Lake
│   ├── minio_client.py          # Wrapper boto3 para uploads/downloads no MinIO
│   ├── polars_client.py         # Lê tabelas Delta do MinIO com Polars
│   ├── spark_client.py          # Lê CSVs brutos do MinIO — só I/O, sem lógica de negócio
│   └── s3_client.py             # Faz upload do CSV de saída para o MinIO/S3
└── domain/pricing/              # Nomes de negócio — lógica pura, zero I/O
    ├── models.py                # Dataclasses: ElasticityResult, RegressionFit, SeasonalKey
    ├── master_dataset.py        # Função pura: join/filtro/agregação dos 5 DataFrames da Olist
    └── elasticity.py            # Funções puras: regressão, sazonalidade, tiers, imputação
dags/
└── olist_medallion.py           # O único arquivo específico do Airflow em todo o projeto
```

Cada task da DAG segue o padrão **I/O sandwich**:

```python
@task
def silver() -> None:
    # READ — client de infraestrutura busca os dados brutos
    orders = SparkClient(spark).read_csv(bucket, "olist_orders_dataset.csv")
    ...
    # TRANSFORM — função pura de domínio, sem I/O, totalmente testável em unit tests
    master = build_master_dataset(orders, items, customers, products, translation,
                                   start_date=start_date, end_date=end_date)
    # WRITE — client de infraestrutura persiste o resultado
    DeltaClient(spark).write(master, silver_path)
```

Essa separação garante que o domínio tem **zero I/O** — pode ser testado em unit tests sem Docker, sem MinIO e sem um cluster Spark rodando.

### Decisões de Engenharia que Vale Destacar

**Por que Spark para Bronze → Silver?**
O dataset bruto da Olist abrange cinco CSVs com ~100k pedidos que precisam ser joinados, filtrados e agregados. Spark é a ferramenta certa para joins distribuídos nessa escala e reflete o tipo de decisão consciente de custo que tomo em produção: usar o motor distribuído onde o volume de dados justifica.

**Por que Polars para Silver → Gold?**
Após a agregação com Spark, a camada silver tem tamanho médio e cabe confortavelmente em um único nó. Polars é significativamente mais rápido que Pandas para as operações analíticas da camada gold e evita o overhead de subir um job Spark para trabalho que não precisa disso.

**Por que o domínio tem zero I/O?**
`domain/` contém funções Python puras sem chamadas a banco, sem leitura de arquivos, sem contexto Spark. Se uma função de regressão está errada, você descobre em milissegundos com `pytest`, não após um job Spark de 10 minutos.

**Por que `dags/olist_medallion.py` é o único arquivo do Airflow?**
Se eu precisar trocar o Airflow por Prefect ou Dagster amanhã, apenas esse arquivo muda. O domínio e a infraestrutura não têm dependência de orquestrador.

**Por que `infrastructure/` em vez de `adapters/`?**
`adapters/` remete à Arquitetura Hexagonal com suas convenções específicas. Este codebase usa a intenção do padrão (separar I/O de lógica) sem reivindicar o padrão completo. `infrastructure/` diz exatamente o que é.

### Rodando Este Projeto

```bash
make init       # copia .env.example → .env, cria os diretórios necessários
# edite o .env com suas credenciais do Kaggle
make up         # constrói as imagens e sobe todos os containers
make logs       # acompanha os logs dos containers
```

UIs após subir:
- Airflow: `localhost:8080` (admin / admin)
- MinIO: `localhost:9001` (minioadmin / minioadmin123)
- Spark Master: `localhost:8081`

```bash
make test-unit  # testes de domínio — sem Docker
make test-int   # testes de integração — requer containers rodando
```

### Aviso sobre VPN / Rede Corporativa

Se você estiver rodando este projeto atrás de uma VPN corporativa com inspeção SSL ativa, duas coisas podem falhar:

**1. Build do Docker — JARs do Spark**

A imagem do Spark anteriormente baixava quatro JARs do Maven Central durante o build. O Dockerfile já foi atualizado para usar `COPY` — os JARs estão pré-baixados e commitados em `docker/spark/jars/`. Nenhum acesso de rede é necessário para o build da imagem Spark.

**2. Runtime da DAG — download do Kaggle**

A task bronze baixa o dataset da Olist do Kaggle via `kagglehub`. Se sua VPN interceptar HTTPS, isso vai falhar com erro de certificado SSL. Rode este projeto em uma máquina sem VPN com inspeção SSL, ou injete o certificado CA corporativo nas imagens Docker.

### Em Construção

Este portfólio está sendo construído ativamente. A plataforma descrita acima é funcional, mas continuo adicionando projetos, melhorando a cobertura de testes e documentando as decisões de engenharia com mais profundidade. Novos pipelines e casos de uso serão adicionados ao longo do tempo.
