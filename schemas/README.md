# Shared StockTrade contract

`StockTrade.avsc` is authoritative. Java 21 and Avro Maven plugin 1.12.0 generate SpecificRecord classes with LocalDate
and BigDecimal logical types into `target/generated-sources/avro`. Generated source is not committed.

```sh
mvn install
```

This installs `com.example.finance:stock-trade-schema:1.0.0-SNAPSHOT` for the service builds. The artifact includes the
source schema as a resource. Run this build before building `ms_trader` from a fresh checkout; rerun it whenever the
contract changes. Keep the artifact version aligned with consumers.

Compose's schema-init job registers this same file as `stock-trades-value` and sets BACKWARD_TRANSITIVE explicitly.
Producers use TopicNameStrategy and disable automatic registration. New incompatible contracts require review and a
versioned migration; changing only the local Java artifact is insufficient.
