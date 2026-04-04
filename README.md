# Data Projects Portfolio

## English

### About This Repository

This repository is my public portfolio for data analytics, data engineering, and machine learning projects.

It brings together code, pipelines, experiments, and end-to-end solutions adapted from real-world business scenarios and rebuilt with public or synthetic data whenever needed for public sharing.

The goal of this repository is to show how I approach data problems in practice, from data ingestion and transformation to analysis, modeling, orchestration, and delivery.

### What You Will Find Here

- Data engineering pipelines and workflow orchestration projects
- Analytics projects focused on business metrics, reporting, and decision support
- Machine learning projects, experiments, and applied use cases
- Structured code adapted for portfolio presentation and public review

### Repository Purpose

This repository was created to document and showcase practical work that reflects real business contexts while staying appropriate for public exposure.

Rather than publishing raw internal material, I adapt ideas, structures, and implementation patterns into portfolio-ready projects that preserve the technical reasoning without exposing confidential information.

### Engineering Standards

Whenever the project context fits, I structure code with Domain-Driven Design (DDD) principles to keep business rules explicit, isolated, and easier to evolve.

I also use Test-Driven Development (TDD) as a working standard in many projects, especially where business logic and pipeline orchestration require clear contracts, regression safety, and maintainable tests.

Part of the architecture standard used in this repository was designed based on the software development ideas explained in the book [*Cosmic Python*](https://www.cosmicpython.com/book), especially around Domain-Driven Design and Test-Driven Development. I adapted those ideas to fit data pipelines and workflow-oriented projects rather than full application software.

### Pipeline Structure

For data pipeline and Airflow-oriented projects, I follow a layered structure designed to separate orchestration, domain logic, contracts, and infrastructure responsibilities.

Typical project organization includes:

- `main.py` for orchestration only
- `core/domain.py` for pure business logic and transformations
- `core/schemas.py` for typed internal contracts, usually implemented with dataclasses
- `core/infrastructure.py` for external I/O such as queries, storage, and persistence
- `tests/` for orchestration and domain-level test coverage

Pipeline definitions are configuration-driven through `dag.yaml`, while the Python modules focus on execution flow and implementation details instead of defining Airflow DAG objects directly in code.

### Work In Progress

This portfolio is still being built.

I am gradually adapting, organizing, and publishing projects here, so this repository does not yet include all of my work. Some projects will be added over time as the code is cleaned up, generalized, and prepared for public release.

### Notes

- The projects in this repository are selected examples, not a complete archive of everything I have worked on
- Some implementations are inspired by real business scenarios, but the published versions use public or synthetic data
- The focus is on demonstrating technical thinking, project structure, and practical problem-solving

### Repository Evolution

As this portfolio grows, new projects, pipelines, notebooks, and supporting materials will be added to provide a broader view of my work across analytics, data engineering, and machine learning.

---

## Português

### Sobre Este Repositório

Este repositório é meu portfólio público de projetos de analytics, engenharia de dados e machine learning.

Aqui eu reúno códigos, pipelines, experimentos e soluções de ponta a ponta adaptadas de cenários reais de negócio e reconstruídas com dados públicos ou sintéticos sempre que necessário para compartilhamento público.

O objetivo deste repositório é mostrar como eu estruturo e resolvo problemas de dados na prática, desde ingestão e transformação até análise, modelagem, orquestração e entrega.

### O Que Você Vai Encontrar Aqui

- Pipelines de engenharia de dados e projetos de orquestração de workflows
- Projetos de analytics com foco em métricas de negócio, reporting e apoio à decisão
- Projetos de machine learning, experimentos e casos de uso aplicados
- Código organizado e adaptado para apresentação em portfólio e avaliação pública

### Propósito do Repositório

Este repositório foi criado para documentar e apresentar trabalhos práticos que refletem contextos reais de negócio, mas de forma apropriada para exposição pública.

Em vez de publicar materiais internos de forma bruta, eu adapto ideias, estruturas e padrões de implementação para projetos prontos para portfólio, preservando o raciocínio técnico sem expor informações confidenciais.

### Padrões de Engenharia

Sempre que o contexto do projeto permite, eu estruturo o código com princípios de Domain-Driven Design (DDD) para manter as regras de negócio mais explícitas, isoladas e fáceis de evoluir.

Também utilizo Test-Driven Development (TDD) como padrão de trabalho em muitos projetos, principalmente quando a lógica de negócio e a orquestração dos pipelines exigem contratos claros, segurança contra regressões e testes de manutenção simples.

Parte do padrÃ£o de arquitetura usado neste repositÃ³rio foi desenhada com base nas ideias de desenvolvimento de software explicadas no livro [*Cosmic Python*](https://www.cosmicpython.com/book), especialmente em relaÃ§Ã£o a Domain-Driven Design e Test-Driven Development. Eu adaptei essas ideias para funcionar em pipelines de dados e projetos orientados a workflows, em vez de softwares completos tradicionais.

### Estrutura de Pipelines

Para projetos de pipeline de dados e estruturas orientadas a Airflow, eu sigo uma organização em camadas para separar responsabilidades entre orquestração, lógica de domínio, contratos internos e infraestrutura.

A estrutura base normalmente inclui:

- `main.py` para orquestração
- `core/domain.py` para lógica de negócio e transformações puras
- `core/schemas.py` para contratos tipados internos, normalmente com dataclasses
- `core/infrastructure.py` para I/O externo, como consultas, armazenamento e persistência
- `tests/` para testes de orquestração e de domínio

As definições das pipelines são orientadas por configuração via `dag.yaml`, enquanto o código Python fica responsável pelo fluxo de execução e pela implementação, sem criar objetos de DAG do Airflow diretamente dentro dos módulos da pipeline.

### Em Construção

Este portfólio ainda está em construção.

Estou adaptando, organizando e publicando projetos gradualmente, então este repositório ainda não contém todo o meu trabalho. Alguns projetos serão adicionados ao longo do tempo conforme o código for sendo revisado, generalizado e preparado para publicação.

### Observações

- Os projetos deste repositório são exemplos selecionados e não um arquivo completo de tudo o que eu já desenvolvi
- Algumas implementações foram inspiradas em cenários reais de negócio, mas as versões publicadas utilizam dados públicos ou sintéticos
- O foco aqui é demonstrar raciocínio técnico, estrutura de projeto e resolução prática de problemas

### Evolução do Repositório

Conforme este portfólio evoluir, novos projetos, pipelines, notebooks e materiais de apoio serão adicionados para ampliar a visão sobre meu trabalho em analytics, engenharia de dados e machine learning.
