#!/usr/bin/env bash
set -euo pipefail
for topic in stock-trades stock-trades-processed stock-trades-dlt; do
retention=604800000
if [ "$topic" = stock-trades-dlt ]; then retention=1209600000; fi
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic "$topic" --partitions 6 --replication-factor 1 \
  --config min.insync.replicas=1 --config retention.ms="$retention" --config cleanup.policy=delete
info=$(kafka-topics --bootstrap-server kafka:29092 --describe --topic "$topic")
printf '%s\n' "$info"
printf '%s\n' "$info" | grep -Eq 'PartitionCount: *6[[:space:]]+ReplicationFactor: *1([[:space:]]|$)'
config=$(kafka-configs --bootstrap-server kafka:29092 --entity-type topics --entity-name "$topic" --describe --all)
for expected in "retention.ms=$retention" 'cleanup.policy=delete' 'min.insync.replicas=1'; do
  printf '%s\n' "$config" | awk -v expected="$expected" '$1 == expected {found=1} END {exit !found}'
done
done
