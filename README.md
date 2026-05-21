# Developer Metrics Pipeline

Pipeline de processamento assíncrono de métricas de desenvolvedores utilizando Golang, AWS SQS, DynamoDB, Docker e LocalStack.

---

# Arquitetura

```txt
[SQS: raw-events]
        ↓
[Processor Service]
        ↓
[SQS: processed-events]
        ↓
[Aggregator Service]
        ↓
[DynamoDB]
        ↓
[REST API]
```

---

# Objetivo

O sistema recebe eventos de métricas de desenvolvedores através de filas SQS, processa os dados, agrega informações e disponibiliza uma API REST para consulta.

O projeto foi desenvolvido utilizando arquitetura orientada a eventos e comunicação assíncrona entre serviços.

---

# Tecnologias Utilizadas

- Golang 1.23
- Docker
- Docker Compose
- AWS SDK Go v2
- LocalStack
- Amazon SQS
- DynamoDB

---

# Estrutura do Projeto

```txt
services/
 ├── processor/
 │    ├── cmd/
 │    ├── go.mod
 │    └── Dockerfile
 │
 └── aggregator/
      ├── cmd/
      ├── go.mod
      └── Dockerfile

infra/
 └── localstack/
      └── init-aws.sh

scripts/
 └── seed.sh

docker-compose.yml
README.md
```

---

# Componentes

## Processor

Responsável por:

- consumir mensagens da fila `raw-events`
- validar eventos
- enriquecer mensagens
- publicar na fila `processed-events`

---

## Aggregator

Responsável por:

- consumir mensagens da fila `processed-events`
- agregar métricas por desenvolvedor
- persistir eventos no DynamoDB
- expor API REST

---

# Filas SQS

| Fila | Função |
|---|---|
| raw-events | Entrada de eventos brutos |
| processed-events | Eventos processados pelo Processor |
| raw-events-dlq | Dead Letter Queue da raw-events |
| processed-events-dlq | Dead Letter Queue da processed-events |

---

# Estrutura dos Eventos

## Evento recebido

```json
{
  "event_id": "uuid-v4",
  "developer_id": "dev-123",
  "metric_type": "commits",
  "value": 15,
  "repository": "org/repo-name",
  "timestamp": "2026-04-15T10:30:00Z"
}
```

---

## Evento processado

```json
{
  "event_id": "uuid-v4",
  "developer_id": "dev-123",
  "metric_type": "commits",
  "value": 15,
  "repository": "org/repo-name",
  "timestamp": "2026-04-15T10:30:00Z",
  "processed_at": "2026-04-15T10:30:05Z",
  "processor_id": "processor-1"
}
```

---

# Validações Implementadas

O Processor valida:

- `event_id` obrigatório
- `developer_id` obrigatório
- `metric_type` válido:
  - commits
  - pull_requests
  - review_time_minutes
- `value >= 0`
- `review_time_minutes <= 1440`
- timestamp não pode ser futuro

---

# DynamoDB

## Tabela `events`

Armazena todos os eventos processados individualmente.

---

## Tabela `developer_summary`

Armazena métricas agregadas por desenvolvedor.

---

# API REST

## Health Check

```http
GET /health
```

### Resposta

```json
{
  "status": "ok"
}
```

---

## Summary

```http
GET /metrics/dev-1/summary
```

### Resposta

```json
{
  "developer_id":"dev-1",
  "total_commits":210,
  "total_pull_requests":0,
  "avg_review_time_minutes":0,
  "events_processed":20
}
```

---

# Como Executar

## Pré-requisitos

Instalar:

- Docker Desktop
- Go 1.23+

---

# Instalar dependências

## Processor

```bash
cd services/processor
go mod tidy
```

---

## Aggregator

```bash
cd services/aggregator
go mod tidy
```

---

# Subir ambiente

Na raiz do projeto:

```bash
docker compose up --build
```

---

# Verificar containers

```bash
docker ps
```

Containers esperados:

- localstack
- processor
- aggregator

---

# Popular fila com eventos

Executar:

```bash
bash scripts/seed.sh
```

---

# Testar API

## Health

```txt
http://localhost:8080/health
```

---

## Summary

```txt
http://localhost:8080/metrics/dev-1/summary
```

---

# Fluxo Completo

1. Evento enviado para `raw-events`
2. Processor consome evento
3. Evento é validado
4. Evento enriquecido é enviado para `processed-events`
5. Aggregator consome evento
6. Dados são agregados
7. Informações persistidas no DynamoDB
8. API REST disponibiliza os dados

---

# Conceitos Demonstrados

- Arquitetura orientada a eventos
- Processamento assíncrono
- Comunicação entre microsserviços
- Integração com AWS
- Uso de SQS
- Uso de DynamoDB
- Dead Letter Queue (DLQ)
- Docker Compose
- LocalStack
- APIs REST
- Agregação incremental

---

# Melhorias Futuras

Possíveis melhorias futuras:

- Worker pool configurável
- Retry com backoff exponencial
- Logs estruturados em JSON
- OpenTelemetry
- Testes unitários completos
- Swagger/OpenAPI
- Persistência completa do summary no DynamoDB
- CI/CD pipeline
- Makefile
- Idempotência persistente

---

# Decisões Técnicas

O projeto foi dividido em dois serviços independentes para simular uma arquitetura distribuída baseada em eventos.

A comunicação foi realizada exclusivamente via SQS para garantir desacoplamento entre os serviços.

O LocalStack foi utilizado para simular os serviços AWS localmente sem necessidade de conta AWS real.

O Docker Compose foi utilizado para simplificar a execução do ambiente completo com um único comando.

---

# Autor

Projeto desenvolvido para o case técnico de Analista de Engenharia Pleno — AI Coding Tools.