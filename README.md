# Email Engine – Event-Driven Kafka Pipeline

A production-shaped, event-driven email processing pipeline built on **Apache Kafka**.  
Designed to explore Kafka **internals, failure handling, retries, DLQ, backpressure, and replay semantics** using raw Kafka consumers and producers.

---

## Overview

This project implements an email processing engine using Kafka as the backbone.

Instead of treating Kafka as a black box, the system is built using:
- manual consumers & producers
- explicit offset management
- retry and dead-letter routing
- key-based partitioning
- failure simulation and replay

The focus is **correctness under failure**, not feature completeness.

---

## Architecture

REST API
│
▼
email.requests
│
▼
ValidatorWorker
├─▶ email.outbound (valid requests)
└─▶ email.invalid (validation failures)

email.outbound
│
▼
SenderWorker
├─▶ email.retry (transient failures)
└─▶ email.dead (permanent failures)

email.retry
│
▼
RetryWorker
├─▶ email.outbound (retry)
└─▶ email.dead (max retries exceeded)

markdown
Copy code

---

## Kafka Topics

| Topic | Purpose | Partitions |
|------|--------|-----------|
| `email.requests` | Ingress from REST API | 3 |
| `email.outbound` | Validated emails ready to send | 3 |
| `email.retry` | Transient failures | 3 |
| `email.dead` | Dead Letter Queue (DLQ) | 1 |
| `email.invalid` | Validation failures | 1 |

- **Partitions** provide parallelism
- **Consumer groups** provide load sharing
- **Ordering** is guaranteed per partition

---

## Key Design Choices

### Manual Offset Management
- `enable.auto.commit = false`
- Offsets are committed **only after processing and producing downstream events**
- Ensures safe replay and at-least-once delivery

### Retry vs Dead Letter Queue
- Transient failures are retried via a separate topic
- Permanent failures are routed to a DLQ
- Retries are modeled as **state transitions**, not loops

### Backpressure via Lag
- Kafka absorbs load using partition logs
- Slow consumers increase lag instead of dropping data
- Lag is treated as an operational signal, not an error

### Key-Based Partitioning
Messages are produced with a shard key:
key = recipientEmail
This guarantees:

deterministic partitioning

ordering per recipient

safe retries and replay

Workers
EmailProducer
Entry point (REST → Kafka)

Produces to email.requests

Adds message metadata:

message-id

idempotency-key

ValidatorWorker
Stateless validation

Routes messages to:

email.outbound (valid)

email.invalid (invalid)

SenderWorker
Performs the side effect (send email)

Classifies failures:

transient → email.retry

permanent → email.dead

Enforces business idempotency

Uses MailHog as SMTP sink for local testing

RetryWorker
Consumes retry messages

Reads retry metadata from headers

Applies backoff

Re-routes to outbound or DLQ based on retry count

Idempotency
Kafka provides at-least-once delivery.

This project enforces idempotency at two levels:

Producer Idempotence
properties
Copy code
enable.idempotence=true
Prevents duplicate writes during producer retries.

Business Idempotence
Each message carries a message-id

Enforced in SenderWorker

Prevents duplicate emails during replay or retries

Backoff Strategy
Retries are delayed using a simple backoff:

java
Copy code
Thread.sleep(backoffMs);
Backoff slows consumers, not Kafka

Lag increases and drains naturally

Demonstrates real backpressure behavior

Failure Scenarios Covered
Consumer crash mid-batch

Kafka broker restart

Slow consumer causing lag

Offset reset and full replay

Duplicate delivery handling

The system is designed to remain correct under all of these conditions.

Local Setup
Requirements
Docker

Docker Compose

Start the system
bash
Copy code
docker-compose up
MailHog
SMTP: localhost:1025

UI: http://localhost:8025

MailHog is used only as a local email sink.

Kafka CLI Usage
bash
Copy code
# List topics
kafka-topics --bootstrap-server localhost:9093 --list

# Describe topic
kafka-topics --describe --topic email.outbound

# Consume with metadata
kafka-console-consumer \
  --topic email.retry \
  --from-beginning \
  --property print.headers=true \
  --property print.partition=true \
  --property print.offset=true

# Consumer group lag
kafka-consumer-groups \
  --describe \
  --group sender-group