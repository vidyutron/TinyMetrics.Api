# TinyMetrics API

TinyMetrics is an educational project demonstrating the evolution of a high-throughput ingestion API. It showcases the journey from a fragile monolith to a robust, sharded, and durable distributed system.

## 🚀 Project Evolution

This project was built in four distinct phases, each addressing specific scalability and reliability challenges.

### Phase 1: The Monolith (Baseline)
We started with a single ASP.NET Core API writing directly to a single Postgres table.
*   **The Problem**: Under high load, we hit the *Too many clients* error (Postgres connection limit).
*   **The Lesson**: Direct synchronous writes to a DB create a hard ceiling for scalability and make the API fragile.

### 🚀 Phase 2: Decoupling & Batching
We introduced a Producer-Consumer pattern using a custom queue and a Background Worker.
*   **Asynchronous Ingestion**: The API accepts data into a `ConcurrentQueue` and returns `202 Accepted` immediately.
*   **Batching**: The worker waits for 100 items (or a 5s timeout) and performs a single bulk insert.
*   **The Result**: API throughput increased by 10x-100x because it no longer waits for the disk.

### 🛡️ Phase 3: Durability (The Staging Table)
To prevent data loss if the API process crashes, we replaced the in-memory queue with a Durable Staging Table (`Events_Staging`).
*   **The Flow**: API writes to Staging (no indexes, very fast) → Worker reads from Staging → Worker moves to Main.
*   **The Lesson**: This "Landing Zone" pattern ensures we don't lose user data that hasn't been processed yet.

### 🌐 Phase 4: Horizontal Scaling (Sharding)
To handle more data than one machine can store, we split the data across `Shard0` and `Shard1`.
*   **Shard Key**: We used `TenantId` to decide where data lives.
*   **Idempotency**: We implemented `ON CONFLICT DO NOTHING` using a `CorrelationId`.
*   **Consistent Hashing**: We built a Hash Ring with Virtual Nodes to ensure:
    *   **Even Distribution**: Data is spread fairly across shards.
    *   **Stability**: Adding new shards in the future requires moving only a fraction of the data ($1/N$).

## 🛠️ Code Structure

The current codebase represents **Phase 4**.

*   **`TinyMetrics.Api/Program.cs`**: Configures the API and registers services. It implements the "Ingest" endpoint which writes to the Staging table.
*   **`TinyMetrics.Api/ShardProvider.cs`**: Implements **Consistent Hashing**. It maps a `TenantId` to a specific database connection string using a virtual node ring.
*   **`TinyMetrics.Api/ShardIngestWorker.cs`**: A background service that:
    1.  Reads raw batches from `Events_Staging`.
    2.  Deserializes and groups data by the target Shard.
    3.  Inserts data into specific Shard databases in parallel.
    4.  Marks staging data as processed.
*   **`TinyMetrics.Console/`**: A load-testing client that simulates 50,000 requests across 5 different tenants.

## ⚙️ Setup & Configuration

### Prerequisites
*   .NET 9.0 SDK
*   PostgreSQL

### Database Setup
You need to create three databases (`metrics_monolith`, `metrics_shard_0`, `metrics_shard_1`) and the following tables.

**1. Monolith / Staging Database (`metrics_monolith`)**
Used for the "Landing Zone" (Phase 3).
```sql
CREATE TABLE Events_Staging (
    Id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    Content JSONB NOT NULL,
    IsProcessed BOOLEAN DEFAULT FALSE
);
```

**2. Shard Databases (`metrics_shard_0`, `metrics_shard_1`)**
Run this SQL on **both** shard databases.
```sql
CREATE TABLE Events (
    CorrelationId UUID PRIMARY KEY,
    TenantId TEXT NOT NULL,
    EventType TEXT NOT NULL,
    Payload JSONB
);
```

### Configuration
Ensure `appsettings.json` in `TinyMetrics.Api` points to your local Postgres instance:
```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Port=5432;Database=metrics_monolith;Username=postgres;Password=yourpassword;",
    "Shard0": "Host=localhost;Port=5432;Database=metrics_shard_0;Username=postgres;Password=yourpassword;",
    "Shard1": "Host=localhost;Port=5432;Database=metrics_shard_1;Username=postgres;Password=yourpassword;"
  }
}
```

## ▶️ How to Run

1.  **Start the API**:
    ```bash
    cd TinyMetrics.Api
    dotnet run
    ```
    The API will listen on `https://localhost:7121` (check launch settings if different).

2.  **Run the Load Test**:
    Open a new terminal and run the console app to simulate traffic.
    ```bash
    cd TinyMetrics.Console
    dotnet run
    ```