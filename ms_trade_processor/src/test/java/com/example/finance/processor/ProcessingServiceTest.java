package com.example.finance.processor;

import com.example.finance.avro.Side;
import com.example.finance.avro.StockTrade;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProcessingServiceTest {
    @SuppressWarnings("unchecked")
    KafkaTemplate<Object, Object> producer = mock(KafkaTemplate.class);

    static StockTrade event() {
        return StockTrade.newBuilder().setTradeId(UUID.randomUUID().toString()).setTradeDate(LocalDate.of(2026, 10, 24)).setSide(Side.BUY).setSymbol("AAPL").setQuantity(Long.MAX_VALUE).setPrice(new BigDecimal("999999999999.999999")).setCurrency("USD").setCounterparty(null).build();
    }

    @Test
    void sendsToRoutedPartitionBeforeCompletionAndSkipsCompletedDuplicate() throws Exception {
        Store store = new Store();
        var meters = new SimpleMeterRegistry();
        when(producer.partitionsFor("stock-trades-processed")).thenReturn(Collections.nCopies(6, null));
        when(producer.send(any(ProducerRecord.class))).thenAnswer(call -> {
            ProducerRecord<Object, Object> output = call.getArgument(0);
            assertEquals(0, store.completions);
            assertEquals(1, output.partition());
            assertEquals("AAPL", output.key());
            return CompletableFuture.completedFuture(new SendResult<>(output, new RecordMetadata(new TopicPartition(output.topic(), 1), 42, 0, 0, 0, 0)));
        });
        var service = new ProcessingService(store, producer, meters);
        var record = new ConsumerRecord<>("stock-trades", 0, 7, "AAPL", event());
        record.headers().add("X-Correlation-ID", null);
        service.consume(record);
        service.consume(record);
        assertEquals(1, store.completions);
        assertEquals(0, store.releases);
        assertEquals(1, meters.get("trades.processed").counter().count());
        assertEquals(1, meters.get("trades.deduplicated").counter().count());
        verify(producer, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void failedSendCannotMarkComplete() {
        Store store = new Store();
        when(producer.partitionsFor("stock-trades-processed")).thenReturn(Collections.nCopies(6, null));
        when(producer.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        assertThrows(Exception.class, () -> new ProcessingService(store, producer, new SimpleMeterRegistry()).consume(new ConsumerRecord<>("stock-trades", 0, 7, "AAPL", event())));
        assertEquals(0, store.completions);
        assertEquals(1, store.releases);
    }

    @Test
    void invalidKeyOrFieldsDoNotReachPersistenceOrPublication() {
        Store store = new Store();
        var service = new ProcessingService(store, producer, new SimpleMeterRegistry());
        StockTrade valid = event();
        assertThrows(InvalidTradeException.class, () -> service.consume(new ConsumerRecord<>("stock-trades", 0, 0, "MSFT", valid)));
        valid.setQuantity(0L);
        assertThrows(InvalidTradeException.class, () -> service.consume(new ConsumerRecord<>("stock-trades", 0, 0, "AAPL", valid)));
        assertEquals(0, store.claims);
        verifyNoInteractions(producer);
    }

    @Test
    void canonicalUnicodeCounterpartyMatchesProducerValidation() {
        StockTrade event = event();
        event.setCounterparty("\u2003");
        assertEquals("\u2003", TradeData.validate(event, "AAPL").counterparty());
        event.setCounterparty("  demo  ");
        assertThrows(InvalidTradeException.class, () -> TradeData.validate(event, "AAPL"));
    }

    @Test
    void hashAndLogicalValuesAreStable() {
        StockTrade value = event();
        var data = TradeData.validate(value, "AAPL");
        assertEquals(value.getPrice(), data.price());
        assertEquals(Long.MAX_VALUE, data.quantity());
        assertEquals(data.digest(), TradeData.validate(data.event(), "AAPL").digest());
        value.setQuantity(1L);
        assertNotEquals(data.digest(), TradeData.validate(value, "AAPL").digest());
    }

    static class Store implements ProcessingStore {
        int claims, completions, releases;
        boolean done;

        public Claim claim(TradeData data, ProcessedTrade.Point source, String correlation, Instant now) {
            claims++;
            return new Claim(new ProcessedTrade(data.tradeId(), data.digest(), data, done ? "COMPLETE" : "PENDING", source, null, 1, 1, correlation, now, now, "token", now.plusSeconds(60)), "token", done);
        }

        public void complete(Claim claim, ProcessedTrade.Point output, Instant now) {
            completions++;
            done = true;
            assertEquals(1, output.partition());
        }

        public void release(Claim claim) {
            releases++;
        }
    }
}
