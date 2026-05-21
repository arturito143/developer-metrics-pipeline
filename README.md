# Developer Metrics Pipeline

Pipeline de processamento assíncrono de métricas de desenvolvedores utilizando Golang, AWS SQS, DynamoDB, Docker e LocalStack.

---

## Arquitetura

```txt
[SQS: raw-events] → [Processor Service] → [SQS: processed-events] → [Aggregator Service] → [DynamoDB] → [REST API]
```

---

## Tecnologias

- Go 1.23
- Docker / Docker Compose
- AWS SDK Go v2
- LocalStack (SQS + DynamoDB)
- Logrus (logs JSON estruturados)

---

## Estrutura do Projeto

```txt
services/
 ├── processor/
 │    ├── cmd/
 │    │    ├── main.go
 │    │    └── main_test.go
 │    ├── go.mod
 │    └── Dockerfile
 │
 └── aggregator/
      ├── cmd/
      │    ├── main.go
      │    └── main_test.go
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

## Filas SQS

| Fila | Função |
|---|---|
| raw-events | Entrada de eventos brutos |
| processed-events | Eventos validados pelo Processor |
| raw-events-dlq | Dead Letter Queue da raw-events (maxReceiveCount: 3) |
| processed-events-dlq | Dead Letter Queue da processed-events (maxReceiveCount: 3) |

---

## Tabelas DynamoDB

| Tabela | Partition Key | Função |
|---|---|---|
| events | event_id (S) | Armazena todos os eventos processados |
| developer_summary | developer_id (S) | Métricas agregadas por desenvolvedor |

---

## Variáveis de Ambiente

### Processor

| Variável | Padrão | Descrição |
|---|---|---|
| `WORKER_COUNT` | `5` | Número de workers no pool |
| `SQS_ENDPOINT` | `http://localstack:4566` | Endpoint do SQS |
| `RAW_QUEUE_URL` | `http://localstack:4566/000000000000/raw-events` | URL da fila de entrada |
| `PROCESSED_QUEUE_URL` | `http://localstack:4566/000000000000/processed-events` | URL da fila de saída |
| `AWS_REGION` | `us-east-1` | Região AWS |

### Aggregator

| Variável | Padrão | Descrição |
|---|---|---|
| `SQS_ENDPOINT` | `http://localstack:4566` | Endpoint do SQS |
| `DYNAMODB_ENDPOINT` | `http://localstack:4566` | Endpoint do DynamoDB |
| `PROCESSED_QUEUE_URL` | `http://localstack:4566/000000000000/processed-events` | URL da fila de entrada |
| `AWS_REGION` | `us-east-1` | Região AWS |

---

## Como Executar

### Pré-requisitos

- Docker Desktop
- Go 1.23+ (para rodar testes localmente)

### Subir o ambiente

```bash
docker compose up --build
```

O LocalStack possui healthcheck configurado — os serviços Processor e Aggregator só iniciam após o LocalStack estar pronto.

### Verificar containers

```bash
docker ps
```

Containers esperados: `localstack`, `processor`, `aggregator`.

---

## Seed (popular fila com eventos)

Após subir o ambiente:

```bash
bash scripts/seed.sh
```

O script envia:
- 20 mensagens válidas com diferentes developers e metric_types
- 4 mensagens inválidas (event_id vazio, event_id não-UUID, metric_type inválido, timestamp futuro)
- 2 mensagens duplicadas (mesmo event_id) para testar idempotência

---

## API REST

### Health Check

```bash
curl http://localhost:8080/health
```

Resposta (verifica conexão com SQS e DynamoDB):

```json
{
  "status": "ok",
  "details": {
    "sqs": "connected",
    "dynamodb": "connected"
  }
}
```

### Summary de um desenvolvedor

```bash
curl http://localhost:8080/metrics/dev-1/summary
```

Resposta:

```json
{
  "developer_id": "dev-1",
  "total_commits": 25,
  "total_pull_requests": 8,
  "avg_review_time_minutes": 45,
  "events_processed": 10,
  "review_time_sum": 90,
  "review_time_count": 2,
  "last_activity": "2026-04-15T10:30:00Z"
}
```

### Todos os eventos de um desenvolvedor

```bash
curl http://localhost:8080/metrics/dev-1
```

Resposta:

```json
[
  {
    "event_id": "uuid-v4",
    "developer_id": "dev-1",
    "metric_type": "commits",
    "value": 15,
    "repository": "org/api",
    "timestamp": "2026-04-15T10:30:00Z",
    "processed_at": "2026-04-15T10:30:05Z",
    "processor_id": "processor-worker-0"
  }
]
```

---

## Verificar DLQs

Para verificar se há mensagens nas Dead Letter Queues:

```bash
# Verificar raw-events-dlq
docker exec developer-metrics-pipeline-localstack-1 awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/raw-events-dlq \
  --attribute-names ApproximateNumberOfMessages

# Verificar processed-events-dlq
docker exec developer-metrics-pipeline-localstack-1 awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/processed-events-dlq \
  --attribute-names ApproximateNumberOfMessages

# Ler mensagens da DLQ (sem remover)
docker exec developer-metrics-pipeline-localstack-1 awslocal sqs receive-message \
  --queue-url http://localhost:4566/000000000000/raw-events-dlq
```

---

## Testes Unitários

```bash
# Testes do Processor (validação de eventos)
cd services/processor && go test ./cmd/ -v

# Testes do Aggregator (lógica de agregação)
cd services/aggregator && go test ./cmd/ -v
```

---

## Funcionalidades Implementadas

- **Worker pool** configurável via `WORKER_COUNT`
- **Validação UUID** para event_id
- **Retry com backoff exponencial** para envio ao SQS
- **Logs estruturados em JSON** com correlation por event_id
- **Graceful shutdown** com drenagem de workers
- **Idempotência** real via verificação no DynamoDB
- **Persistência do summary** na tabela `developer_summary`
- **Endpoints dinâmicos** (`/metrics/:developer_id` e `/metrics/:developer_id/summary`)
- **Média real** de review_time_minutes (não apenas último valor)
- **Campo last_activity** no summary
- **Health check** com verificação de SQS e DynamoDB
- **Dead Letter Queues** com RedrivePolicy (maxReceiveCount: 3)
- **Multi-stage Docker builds** para imagens menores
- **Testes unitários** para validação e agregação

---

## Autor

Artur Tsouza
