# korvet_kafka_redis_service

# Kafka topics service

A Streamlit app that drives a realistic produce-and-consume workload against any Kafka-protocol broker — Apache Kafka itself, or a compatible broker such as Korvet — and charts throughput and end-to-end latency while it runs.

Each generated document is routed to the topic matching its own document type, so the topic split reflects a real field in the data rather than a synthetic partitioning scheme. The producer and consumer run as background threads inside the app process, and the charts read the same in-memory counters those threads write to, so nothing is polled out of the broker just to draw a graph.

## Requirements

- Python 3.9 or newer  
- A reachable Kafka-protocol broker (there is no in-process fallback)  
- `streamlit` and `kafka-python`

## Running the application

```sh
pip install streamlit kafka-python
streamlit run kafka_streamlit_app.py
```

Streamlit prints a local URL when it starts, by default [http://localhost:8501](http://localhost:8501). Open that and drive everything from the page.

## First run

1. **Bootstrap servers** — set this in the sidebar. Default is `localhost:9092`.  
2. **Test connection** — confirms the broker answers before you start any traffic.  
3. **Create this app's topics** — creates the five topics with 3 partitions each, replication factor 1, and a 5 minute retention window. Required on brokers that do not auto-create topics on first produce; safe to press if the topics already exist.  
4. **Start producer**, then **Start consumer**.  
5. Watch the throughput and latency charts, and the per-topic table below them.

**Stop producer**, **Stop consumer** and **Stop all** wind the threads down again; the counters and charts survive so you can inspect a finished run.

Connection settings are locked while either worker is running. Stop both to change them.

**List broker topics** asks the broker what topics it actually has, independently of anything this app produced. On Korvet that is the same query as `korvet topics --list --bootstrap-server <servers>`, and on Kafka the same as `kafka-topics.sh --list`. Useful for confirming topic creation landed, or that you are pointed at the broker you think you are.

## What you see

**Summary cards** — produced, consumed and committed counts with their current per-second rates, plus lag p50, p95 and max, in-flight count, topic count, failed handles and errors.

**Topics table** — per topic: produced, consumed, in flight, failed, last partition, last offset and last seen.

**Throughput chart** — messages per second across all topics combined.

**End-to-end latency chart** — milliseconds from the producer's `send()` to the consumer committing the offset, derived from the `produced_at_ms` field stamped on every record. Gaps in the line mean there was no traffic.

Auto refresh runs every 2 seconds and pauses while both workers are stopped. **Reset metrics** clears the counters and charts without touching the broker.

## Topics

Every document carries a `DocRequestCode`, and that value selects the topic:

| `DocRequestCode` | Topic |
| :---- | :---- |
| `East-Bank` | `bigcreditbank.applications.east-bank` |
| `West-Bank` | `bigcreditbank.applications.west-bank` |
| `North-Bank` | `bigcreditbank.applications.north-bank` |
| `South-Bank` | `bigcreditbank.applications.south-bank` |
| anything else | `bigcreditbank.applications.other` |

Unknown or future codes fall through to the `other` topic instead of raising, so a change on the producing side cannot take the app down. Change the `bigcreditbank.applications` prefix with `KAFKA_TOPIC_PREFIX`.

## Retention

Topics are created with a 5 minute retention window, and the same window is applied to topics that already exist, so changing retention is not limited to a fresh broker.

| Setting | Value | Why |
| :---- | :---- | :---- |
| `retention.ms` | `300000` (5 min) | how long records are kept |
| `segment.ms` | `60000` (retention ÷ 5\) | how often a segment rolls |

`segment.ms` has to stay **below** `retention.ms`. A broker only deletes closed segments, so with Kafka's 7 day default segment roll a 5 minute retention would not actually drop anything — and some brokers reject the pair outright with `InvalidConfigurationError: segment.ms must be less than effective retention`.

## The message

Each record is a JSON envelope wrapping a generated application document:

```json
{
  "payload":           { "...": "the full application document" },
  "request_id":        "...",
  "doc_request_code":  "East-Bank",
  "application_no":    "...",
  "agreement_no":      "...",
  "produced_at_ms":    1789723441552,
  "schema_version":    "1"
}
```

The record key is `request_id`, so records spread across a topic's partitions by key hash. `produced_at_ms` is what the latency chart measures against. Payloads are internally consistent rather than field-by-field noise: identifiers encode the same date of birth, birth state and gender as their sibling fields, postcodes and telephone area codes belong to the chosen state, and every finance figure is derived from the product prices and term so deposits, instalments, interest and balances reconcile.

## Configuration

Everything below is settable in the sidebar. Where a sidebar control exists it wins; the environment variable supplies the starting value.

| Sidebar control | Environment variable | Default |
| :---- | :---- | :---- |
| Bootstrap servers | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| Consumer group | `KAFKA_CONSUMER_GROUP` | `bigcreditbank-doc-processors` |
| Producer client id | — | `dashboard-producer-1` |
| Consumer client id | `KAFKA_CONSUMER_NAME` | blank, auto-filled as `host-pid` |
| API version (blank \= auto-detect) | `KAFKA_API_VERSION` | blank |
| Rate (msg/s) | `KAFKA_PRODUCE_RATE` | `5` |
| Producer acks | `KAFKA_ACKS` | `all` |
| Auto offset reset | `KAFKA_AUTO_OFFSET_RESET` | `earliest` |
| Max records / poll | `KAFKA_MAX_POLL_RECORDS` | `50` |
| Poll (ms) | `KAFKA_POLL_MS` | `2000` |
| Producer max block (ms) | `KAFKA_MAX_BLOCK_MS` | `10000` |

The producer client id is set by the app itself rather than from the environment, so `KAFKA_CLIENT_ID` does not seed it. Edit the sidebar field to change it.

Environment-only:

| Variable | Default | Meaning |
| :---- | :---- | :---- |
| `KAFKA_TOPIC_PREFIX` | `bigcreditbank.applications` | topic name prefix |
| `KAFKA_TOPIC_RETENTION_MS` | `300000` | retention window |
| `KAFKA_TOPIC_SEGMENT_MS` | `60000` | segment roll |
| `KAFKA_FETCH_MIN_BYTES` | `1048576` | see below |
| `KAFKA_FETCH_MAX_WAIT_MS` | `200` | see below |

### Fetch shaping — note the non-standard defaults

`KAFKA_FETCH_MIN_BYTES` and `KAFKA_FETCH_MAX_WAIT_MS` in this bundle default to **1048576 and 200**, not kafka-python's stock 1 and 500\. That is deliberate: it exists so Kafka can be compared against a broker that serves fetches on a fixed cycle rather than on arrival.

With `fetch_min_bytes=1`, a broker answers a fetch the instant the first record lands, and latency is a few milliseconds. Raising the gate to 1 MB puts it out of reach of this workload, so every fetch instead returns when the 200 ms timer expires, handing the consumer a clump of records on a fixed cycle. The median latency then lands near half the wait, around 100 ms.

**This is on by default, and it applies to whatever broker you point at.** If you want stock low-latency behaviour, set:

```sh
KAFKA_FETCH_MIN_BYTES=1 KAFKA_FETCH_MAX_WAIT_MS=500 streamlit run kafka_streamlit_app.py
```

If you are comparing two brokers, shape only one side of the comparison and leave the other at the stock values, or the result measures nothing.

Keep `KAFKA_MAX_POLL_RECORDS` above rate × wait when shaping. At 165 msg/s and a 200 ms wait that is about 33 records, comfortably under the default 50; push the wait to 300 ms or beyond and the clump is truncated, the next poll returns immediately from the client buffer, and the cycle collapses.

## Troubleshooting

**`producer.send()` hangs, then `KafkaTimeoutError: Failed to update metadata`** — the topics do not exist and the broker does not auto-create them. Press **Create this app's topics**.

**`UnsupportedCodecError: Libraries for lz4 compression codec not found`** — the broker is handing back lz4-compressed batches. Install the codec with `pip install lz4`.

**Metadata calls work but produce or consume hangs** — kafka-python's protocol auto-negotiation may not have landed on something the broker implements. Put an explicit version such as `0.10.1` or `2.5.0` in the sidebar's **API version** field.

**Latency looks high (\~100 ms) at low throughput** — that is the fetch shaping above, not the broker. See the stock-values command.

## Repository contents

| File | Role |
| :---- | :---- |
| `kafka_streamlit_app.py` | the application |
| `kafka_workers.py` | producer and consumer worker threads, instrumented into `Metrics` |
| `kafka_producer.py` | produce path and the record envelope |
| `kafka_consumer.py` | poll, handle and commit path |
| `kafka_topics.py` | topic routing, creation and retention |
| `payloads.py` | randomised application document generator |
| `metrics.py` | thread-safe throughput and latency metrics, standard library only |
| `config.py` | shared configuration and client keyword arguments |
| `requirements.txt` | `kafka-python` |
| `tests/` | test suites and in-memory broker and Redis stand-ins |

The bundle also carries a Redis Streams implementation of the same workload (`producer.py`, `consumer.py`, `redis_client.py`, `workers.py`) plus alternative front ends. Those are not needed to run the app.

## Tests

```sh
python run_tests.py
```

Four suites — service logic, client wire protocol, dashboard/metrics/workers, and the Kafka topics path. They run against in-memory stand-ins, so neither a broker nor `kafka-python` needs to be installed.
