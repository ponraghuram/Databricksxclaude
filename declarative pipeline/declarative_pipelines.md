# Declarative Pipelines

## What is a Declarative Pipeline?

A declarative pipeline is a data pipeline approach where you define **what** the output should look like rather than **how** to compute it. You specify the desired end state — the transformations, data flows, and dependencies — and the framework handles the execution, orchestration, and optimization automatically.

In Databricks, Declarative Pipelines (formerly Delta Live Tables / DLT) allow you to define tables and transformations using SQL or Python, and the system manages:
- Dependency resolution between tables
- Incremental processing and checkpointing
- Schema inference and enforcement
- Error handling and data quality monitoring
- Pipeline orchestration and scheduling

---

## Advantages of Declarative Pipelines

### 1. Simplified Development
- Write transformations without worrying about execution order
- No manual dependency management between tables
- Less boilerplate code — focus on business logic

### 2. Automatic Incremental Processing
- The framework automatically determines what data is new and processes only that
- Built-in support for streaming and batch modes
- Checkpointing is handled automatically

### 3. Built-in Data Quality
- Define expectations (constraints) directly in the pipeline
- Automatically quarantine, warn, or fail on bad data
- Quality metrics tracked over time

### 4. Reliability and Fault Tolerance
- Automatic retries on failure
- Transactional writes — no partial updates or corrupt states
- Easy recovery from failures without reprocessing everything

### 5. Observability
- Auto-generated lineage graph showing how tables relate
- Pipeline event logs and metrics built in
- Visual DAG (Directed Acyclic Graph) of all tables and flows

### 6. Scalability
- Runs on serverless or standard compute
- Optimizes execution plans automatically
- Scales with data volume without code changes

### 7. Lower Maintenance
- Schema changes propagate automatically downstream
- No need to manually manage job scheduling between dependent steps

---

## Declarative Pipeline vs Normal (Imperative) Pipeline

| Aspect | Declarative Pipeline | Normal (Imperative) Pipeline |
|---|---|---|
| **Approach** | Define *what* the result should be | Define *how* to compute the result step by step |
| **Execution Order** | Automatically resolved by the framework | Manually specified by the developer |
| **Incremental Processing** | Built-in, automatic | Must be coded manually (watermarks, offsets, etc.) |
| **Dependency Management** | Framework tracks table dependencies | Developer manages task/job dependencies |
| **Data Quality** | Native expectations and quarantine support | Requires custom validation logic |
| **Error Handling** | Automatic retries and fault recovery | Must be implemented manually |
| **Orchestration** | Single pipeline definition runs the whole flow | Multiple jobs/notebooks need external orchestration (e.g., Airflow, Workflows) |
| **Code Complexity** | Low — focus on transformation logic | High — includes orchestration, state management, error handling |
| **Lineage** | Auto-generated and visualized | Not available unless separately configured |
| **Observability** | Built-in metrics, logs, and DAG view | Requires custom logging and monitoring |
| **Schema Management** | Auto-inferred and enforced | Managed manually |
| **Maintenance Effort** | Low — framework handles changes | High — changes require updating multiple jobs |

---

## Example Comparison

### Normal Pipeline (Imperative)
```python
# Step 1: Read raw data
raw_df = spark.read.format("delta").load("/raw/bookings")

# Step 2: Filter only new records manually
last_processed = get_last_watermark()
new_df = raw_df.filter(col("event_time") > last_processed)

# Step 3: Transform
silver_df = new_df.select("booking_id", "city", "amount")

# Step 4: Write and update watermark
silver_df.write.format("delta").mode("append").save("/silver/bookings")
update_watermark(new_df.agg(max("event_time")).collect()[0][0])
```

### Declarative Pipeline (Databricks SDP / DLT)
```python
import dlt

@dlt.table
def silver_bookings():
    return (
        dlt.read_stream("raw_bookings")
           .select("booking_id", "city", "amount")
    )
```

The declarative version is shorter, automatically incremental, and handles checkpointing, retries, and lineage without any extra code.

---

## When to Use Declarative Pipelines

- Building multi-hop data pipelines (Bronze → Silver → Gold)
- Streaming ingestion with automatic checkpointing
- When data quality enforcement is critical
- Teams that want lower operational overhead
- Projects requiring automatic lineage and observability
