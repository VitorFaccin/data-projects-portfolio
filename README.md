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

### Architecture: Cosmic Python Applied to Data Pipelines

I structured this codebase using the architectural patterns from [*Cosmic Python*](https://www.cosmicpython.com/book) by Harry Percival and Bob Gregory — specifically DDD bounded contexts, the ports-and-adapters pattern, and the service layer — adapted for data pipelines instead of web applications.

The result is a clear separation between what the pipeline *does* and how it *does it*:

```
src/
├── domain/pricing/         # Pure business logic — zero I/O, no Spark, no Airflow
│   ├── model.py            # Dataclasses: ElasticityResult, RegressionFit, SeasonalKey
│   └── services.py         # Pure functions: regression, seasonality, imputation
├── adapters/
│   ├── storage/            # Technology names: minio_client.py, delta_repository.py
│   └── processing/         # Technology names: spark_processor.py, polars_processor.py
├── service_layer/
│   └── pricing_pipeline.py # Orchestrates adapters + domain — no Airflow imports
└── entrypoints/            # Thin wrappers called by the DAG
dags/
└── olist_medallion.py      # The only Airflow-specific file in the entire project
```

Key naming convention: `domain/` folders use business names (`pricing/`), `adapters/` folders use technology names (`spark_processor`, `minio_client`). This makes it immediately obvious where business rules live versus infrastructure.

### Engineering Decisions Worth Noting

**Why Spark for Bronze → Silver?**
The raw Olist dataset spans five CSVs with ~100k orders that need to be joined, filtered, and aggregated. Spark is the right tool for distributed joins at this scale, and it mirrors the kind of cost-aware decision I make in production: use the distributed engine where the data justifies it.

**Why Polars for Silver → Gold?**
After the Spark aggregation, the silver layer is medium-sized and single-node friendly. Polars is significantly faster than Pandas for the analytical operations at the gold layer and avoids the overhead of spinning up a Spark job for work that does not need it.

**Why does the domain layer have zero I/O?**
`src/domain/` contains pure Python functions with no database calls, no file reads, no Spark context. This means the domain logic can be unit tested without Docker, without MinIO, and without a running Spark cluster. If a regression function is wrong, you find out in milliseconds with `pytest`, not after a 10-minute Spark job.

**Why is `dags/olist_medallion.py` the only Airflow file?**
If I need to swap Airflow for Prefect or Dagster tomorrow, only that file changes. The service layer, domain, and adapters have no orchestrator dependency.

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

### VPN Warning

If you are running this behind a corporate VPN with SSL inspection enabled, DAG tasks that download data from external sources (Kaggle, Google Storage) will fail with SSL certificate verification errors. Your VPN intercepts HTTPS traffic and re-signs it with a corporate certificate that Docker containers do not trust by default. Run this project on a machine without SSL-intercepting VPN, or configure your corporate CA certificate in the Docker images.

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

### Arquitetura: Cosmic Python Aplicado a Pipelines de Dados

Estruturei este codebase usando os padrões arquiteturais do livro [*Cosmic Python*](https://www.cosmicpython.com/book) de Harry Percival e Bob Gregory — especificamente bounded contexts de DDD, o padrão ports-and-adapters e a service layer — adaptados para pipelines de dados em vez de aplicações web.

O resultado é uma separação clara entre o que o pipeline *faz* e *como* ele faz:

```
src/
├── domain/pricing/         # Lógica de negócio pura — zero I/O, sem Spark, sem Airflow
│   ├── model.py            # Dataclasses: ElasticityResult, RegressionFit, SeasonalKey
│   └── services.py         # Funções puras: regressão, sazonalidade, imputação
├── adapters/
│   ├── storage/            # Nomes de tecnologia: minio_client.py, delta_repository.py
│   └── processing/         # Nomes de tecnologia: spark_processor.py, polars_processor.py
├── service_layer/
│   └── pricing_pipeline.py # Orquestra adapters + domain — sem imports do Airflow
└── entrypoints/            # Wrappers finos chamados pela DAG
dags/
└── olist_medallion.py      # O único arquivo específico do Airflow em todo o projeto
```

Convenção de nomenclatura: pastas em `domain/` usam nomes de negócio (`pricing/`), pastas em `adapters/` usam nomes de tecnologia (`spark_processor`, `minio_client`). Isso torna imediatamente óbvio onde vivem as regras de negócio versus a infraestrutura.

### Decisões de Engenharia que Vale Destacar

**Por que Spark para Bronze → Silver?**
O dataset bruto da Olist abrange cinco CSVs com ~100k pedidos que precisam ser joinados, filtrados e agregados. Spark é a ferramenta certa para joins distribuídos nessa escala e reflete o tipo de decisão consciente de custo que tomo em produção: usar o motor distribuído onde o volume de dados justifica.

**Por que Polars para Silver → Gold?**
Após a agregação com Spark, a camada silver tem tamanho médio e cabe confortavelmente em um único nó. Polars é significativamente mais rápido que Pandas para as operações analíticas da camada gold e evita o overhead de subir um job Spark para trabalho que não precisa disso.

**Por que o domínio tem zero I/O?**
`src/domain/` contém funções Python puras sem chamadas a banco, sem leitura de arquivos, sem contexto Spark. Isso significa que a lógica de domínio pode ser testada em unit tests sem Docker, sem MinIO e sem um cluster Spark rodando. Se uma função de regressão está errada, você descobre em milissegundos com `pytest`, não após um job Spark de 10 minutos.

**Por que `dags/olist_medallion.py` é o único arquivo do Airflow?**
Se eu precisar trocar o Airflow por Prefect ou Dagster amanhã, apenas esse arquivo muda. A service layer, o domínio e os adapters não têm dependência de orquestrador.

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

### Aviso sobre VPN

Se você estiver rodando este projeto atrás de uma VPN corporativa com inspeção SSL ativa, as tasks da DAG que baixam dados de fontes externas (Kaggle, Google Storage) vão falhar com erros de verificação de certificado SSL. A VPN intercepta o tráfego HTTPS e o reassina com um certificado corporativo que os containers Docker não reconhecem por padrão. Rode este projeto em uma máquina sem VPN com inspeção SSL, ou configure o certificado CA corporativo nas imagens Docker.

### Em Construção

Este portfólio está sendo construído ativamente. A plataforma descrita acima é funcional, mas continuo adicionando projetos, melhorando a cobertura de testes e documentando as decisões de engenharia com mais profundidade. Novos pipelines e casos de uso serão adicionados ao longo do tempo.
