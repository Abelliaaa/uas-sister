# Pub-Sub Log Aggregator (Docker Compose)

# Ringkasan

Sistem ini merupakan implementasi Pub-Sub Log Aggregator berbasis arsitektur multi-service menggunakan Docker Compose. Sistem mendukung idempotency, deduplication persisten, serta pemrosesan sinkron dan asinkron menggunakan Redis sebagai message broker.

## Arsitektur

- **aggregator**: REST API untuk publish event dan query data
- **publisher**: simulator pengirim event (termasuk duplikasi)
- **broker**: Redis sebagai message broker internal
- **storage**: PostgreSQL sebagai deduplication store persisten

Semua service berjalan di jaringan internal Docker Compose.

# Mode Operasi

Sistem mendukung dua mode:

- **SYNC mode** (`ASYNC_MODE=false`): pemrosesan langsung, digunakan untuk testing
- **ASYNC mode** (`ASYNC_MODE=true`): event dikirim ke Redis queue dan diproses worker

# Menjalankan Sistem

```bash
docker compose up --build

# Anggregator dapat diakses di
http://localhost:8080

# ENDPOINT API
POST/publish : Menerima batch event
{
  "events": [
    {
      "topic": "log",
      "event_id": "evt-1",
      "timestamp": "2025-01-01T00:00:00Z",
      "source": "publisher",
      "payload": {}
    }
  ]
}

GET /events?topic=log : Menampilkan event unik per topik.
GET /stats : Menampilkan statistik pemrosesan event.

Testing
ASYNC_MODE=false docker compose up -d
pytest -v


Catatan Desain
Deduplication dilakukan menggunakan constraint unik (topic, event_id)
Transaksi database menggunakan PostgreSQL dengan isolation level default (READ COMMITTED)
Sistem menjamin at-least-once delivery tanpa double processing

```

LINK Laporan : https://docs.google.com/document/d/1WL1iLZXFxV_koUuG6WnYXIHS_7EkjIipY0MO_fSgesg/edit?usp=sharing
LINK Video Demo :  
