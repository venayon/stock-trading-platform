# Scheduled trade feed producer

Independent Spring Boot 3.5 / Spring Batch 5 application, Java 21, port **8083**.

- Scans configurable shared storage every two hours; REST can trigger it on demand.
- Streams and validates CSV before accepting any trades.
- Configurable chunks and inclusive invalid-row count/percentage limits.
- Uses the existing `trader.trades` MongoDB collection and Avro/Kafka outbox implementation.
- PostgreSQL stores Batch job tables/checkpoints and coordinates scans.

See
the [beginner guide, mindmap, commands and restart rules](../../docs/guides/SPRING_BATCH_FEED.md), [HLD/LLD](../../docs/design/FEED_PRODUCER.md),
and [ADR 0021](../../docs/adr/0021-scheduled-feed-producer.md).

Build schema and `ms_trader` with `mvn install` first to provide the schema and attached `core` dependency, then run
`mvn verify` here. Start infrastructure with `python3 scripts/init-monitoring-env.py` and `docker compose up -d` from
`infra`. Start this app **from this directory** with:

```sh
java -Xms64m -Xmx192m -jar target/feed-producer-0.0.1-SNAPSHOT.jar
```

Open [Swagger](http://localhost:8083/swagger-ui.html). Submit `POST /api/feeds/run` with `{}` or
`{"fileName":"orders.csv"}`. Files need a matching `orders.csv.ready` marker in `var/feeds/inbox`. A queued scan is not
yet an accepted feed; poll the returned status URL, then inspect its execution IDs.
