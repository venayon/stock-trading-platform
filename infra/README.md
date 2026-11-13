# Local trading infrastructure

Requires Docker Desktop (or a reachable Docker engine). Start the complete current stack:

```sh
python3 scripts/init-monitoring-env.py
docker compose config --quiet
docker compose up -d --wait
docker compose ps -a
```

| Service                 | Host endpoint                    | Container endpoint          |
|-------------------------|----------------------------------|-----------------------------|
| MongoDB 7.0.16          | mongodb://localhost:27017/trader | mongodb:27017               |
| Kafka / Confluent 7.9.4 | localhost:9092                   | kafka:29092                 |
| Schema Registry 7.9.4   | http://localhost:8085            | http://schema-registry:8081 |
| Kafdrop 4.1.0           | http://localhost:9000            | http://kafdrop:9000         |
| Prometheus 2.55.1       | http://localhost:9090            | http://prometheus:9090      |
| Grafana 11.6.0          | http://localhost:3000            | http://grafana:3000         |

The host-run trader and processor use the host endpoints. All published ports bind to loopback; Kafka/Registry/Mongo are
unauthenticated for this local demo. Kafka is a single combined KRaft broker/controller, not a high-availability
cluster. Its controller listener is not published.

Named volumes `kafka-data` and `mongo-data` retain events, Registry's internal `_schemas` topic and accepted trades
across restarts. Regular stop/start preserves data; deleting volumes resets it. `MONGODB_PORT` can override Mongo's host
port; adjust the application's URI accordingly. Kafka's advertised host listener is explicitly localhost:9092.

The topic-init job creates `stock-trades` and `stock-trades-processed`: six partitions, replication factor/min ISR 1,
delete cleanup, seven-day
retention. It validates existing settings and fails on mismatch rather than silently repartitioning or overwriting
policy. The `stock-trades-dlt` topic uses the same six-partition/RF policy with 14-day retention. Broker automatic topic
creation is disabled. The schema-init job then registers `schemas/StockTrade.avsc` and
applies BACKWARD_TRANSITIVE to source, processed and valid-Avro DLT subjects. Raw malformed DLT bytes do not need schema
decoding. Both jobs should show Exited (0); this is success, not a failed
long-running service. Rerunning initialization is safe.

Kafdrop starts after bootstrap and defaults to AVRO values / DEFAULT string keys.
Open [stock-trades](http://localhost:9000/topic/stock-trades), choose View Messages, select the partition/offset shown
in the trade's `event`, and use value format AVRO and key format
DEFAULT. [All messages](http://localhost:9000/topic/stock-trades/allmessages) also works. Avro's logical decimal/date
may appear in their underlying bytes/epoch-day representation; exact decoded values are verified by the SpecificRecord
integration tests. This pinned Kafdrop version's HTML inspection routes were tested; do not assume every JSON endpoint
documented on its latest development branch exists here.

Healthchecks for Kafka, Registry, Kafdrop and MongoDB have been verified against these images. The app runs on the host
in this slice. Prometheus scrapes the host-run app through host.docker.internal:8081. Grafana
provisions [Stock Trading Overview](http://localhost:3000/d/stock-trading-overview); login is admin and the generated
password lives in ignored infra/.env. Named volumes retain monitoring state. See
the [observability runbook](../docs/design/OBSERVABILITY.md). The producer and processor both run as host Java processes
on ports 8081 and 8082;
see [HLD](../docs/design/HLD.md).

The processor consumes group stock-trade-processor-v1, persists to processor.processed_trades, and routes BUY to output
partitions 0–2 and SELL to 3–5. See the [processor README](../ms_trade_processor/README.md). Prometheus now scrapes both
host-run services, and Grafana has 20 panels including committed group lag and DLT rates.

## Spring Batch job database

`feed-postgres` uses PostgreSQL 16 Alpine on localhost:5433, database/user `feed_batch`, with a durable named volume.
Run `python3 scripts/init-monitoring-env.py` to add its missing password to ignored `.env`, then
`docker compose up -d feed-postgres`. Do not print or commit `.env`. The
new [feed app](../ms_trader/feed-producer/README.md) runs separately on 8083;
its [guide and mindmap](../docs/guides/SPRING_BATCH_FEED.md) explain job tables and recovery. Prometheus includes a
scrape target on 8083.
