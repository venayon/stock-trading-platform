package com.example.finance.processor;

import com.example.finance.avro.StockTrade;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api")
public class ProcessorApi {
    private final MongoTemplate mongo;
    private final KafkaTemplate<Object, Object> producer;

    public ProcessorApi(MongoTemplate mongo, KafkaTemplate<Object, Object> producer) {
        this.mongo = mongo;
        this.producer = producer;
    }

    static String id(String id) {
        if (id == null || !id.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))
            throw new IllegalArgumentException("tradeId must be a UUID");
        return UUID.fromString(id).toString();
    }

    @GetMapping("/processed/{id}")
    @Operation(summary = "Get processing state and routed output coordinates")
    public TradeView find(@PathVariable String id) {
        var found = mongo.findById(id(id), ProcessedTrade.class);
        if (found == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Processed trade not found");
        return TradeView.from(found);
    }

    @GetMapping("/processed")
    @Operation(summary = "Query processed trades with bounded pagination")
    public Page list(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size,
                     @RequestParam(required = false) String symbol, @RequestParam(required = false) String side,
                     @RequestParam(required = false) String processingStatus) {
        if (page < 0 || size < 1 || size > 100)
            throw new IllegalArgumentException("page must be nonnegative; size must be 1..100");
        Query query = new Query();
        if (symbol != null) {
            symbol = symbol.trim().toUpperCase(Locale.ROOT);
            if (!symbol.matches("[A-Z0-9][A-Z0-9.-]{0,15}")) throw new IllegalArgumentException("Invalid symbol");
            query.addCriteria(Criteria.where("trade.symbol").is(symbol));
        }
        if (side != null) {
            if (!Set.of("BUY", "SELL").contains(side)) throw new IllegalArgumentException("Invalid side");
            query.addCriteria(Criteria.where("trade.side").is(side));
        }
        if (processingStatus != null) {
            if (!Set.of("PENDING", "COMPLETE").contains(processingStatus))
                throw new IllegalArgumentException("Invalid processing status");
            query.addCriteria(Criteria.where("processingStatus").is(processingStatus));
        }
        long total = mongo.count(query, ProcessedTrade.class);
        var items = mongo.find(query.with(Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("tradeId"))).skip((long) page * size).limit(size), ProcessedTrade.class);
        return new Page(items.stream().map(TradeView::from).toList(), page, size, total, (total + size - 1) / size);
    }

    @PostMapping("/process")
    @Operation(summary = "Re-enqueue a retained published trade; COMPLETE trades are a no-op",
        description = "202 acknowledges source enqueue only. Replay cannot repair malformed bytes or overwrite a conflicting processed identity.")
    @ApiResponse(responseCode = "202", description = "Canonical source event enqueued; poll GET for completion", content = @Content(schema = @Schema(implementation = ReplayAccepted.class)))
    @ApiResponse(responseCode = "200", description = "Already COMPLETE; no enqueue", content = @Content(schema = @Schema(implementation = TradeView.class)))
    @ApiResponse(responseCode = "400", description = "Invalid request", content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "404", description = "Canonical source not found")
    @ApiResponse(responseCode = "409", description = "Canonical source has not been published")
    @ApiResponse(responseCode = "503", description = "Dependency unavailable; retry later")
    public ResponseEntity<?> replay(@RequestBody ReplayRequest input) throws Exception {
        String id = id(input.tradeId());
        var processed = mongo.findById(id, ProcessedTrade.class);
        if (processed != null && processed.processingStatus().equals("COMPLETE"))
            return ResponseEntity.ok(TradeView.from(processed));
        // Read-only source access: never update trader-owned documents.
        Document source = mongo.getMongoDatabaseFactory().getMongoDatabase("trader").getCollection("trades").find(new Document("_id", id)).first();
        if (source == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source trade not found");
        if (!"PUBLISHED".equals(source.getString("publicationStatus")))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Source trade has not been published");
        var trade = StockTrade.newBuilder().setTradeId(id).setTradeDate(java.time.LocalDate.parse(source.getString("tradeDate")))
            .setSide(com.example.finance.avro.Side.valueOf(source.getString("side"))).setSymbol(source.getString("symbol"))
            .setQuantity(source.getLong("quantity")).setPrice(source.get("price", org.bson.types.Decimal128.class).bigDecimalValue())
            .setCurrency(source.getString("currency")).setCounterparty(source.getString("counterparty")).build();
        TradeData.validate(trade, trade.getSymbol());
        var record = new org.apache.kafka.clients.producer.ProducerRecord<Object, Object>("stock-trades", trade.getSymbol(), trade);
        record.headers().add("replay", "true".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        record.headers().add("X-Correlation-ID", UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var ack = producer.send(record).get(11, TimeUnit.SECONDS).getRecordMetadata();
        return ResponseEntity.accepted().body(new ReplayAccepted(id, new ProcessedTrade.Point(ack.topic(), ack.partition(), ack.offset())));
    }

    public record TradeView(String tradeId, String processingStatus, TradeDataView trade, ProcessedTrade.Point source,
                            ProcessedTrade.Point output, int targetPartition, int routeVersion, Instant createdAt,
                            Instant updatedAt) {
        static TradeView from(ProcessedTrade p) {
            return new TradeView(p.tradeId(), p.processingStatus(), new TradeDataView(p.trade()), p.source(), p.output(), p.targetPartition(), p.routeVersion(), p.createdAt(), p.updatedAt());
        }
    }

    public record TradeDataView(String tradeDate, String side, String symbol, long quantity, String price,
                                String currency, String counterparty) {
        TradeDataView(TradeData d) {
            this(d.tradeDate(), d.side(), d.symbol(), d.quantity(), d.price().toPlainString(), d.currency(), d.counterparty());
        }
    }

    public record Page(List<TradeView> items, int page, int size, long totalElements, long totalPages) {
    }

    public record ReplayRequest(String tradeId) {
    }

    public record ReplayAccepted(String tradeId, ProcessedTrade.Point replayPublication) {
    }
}
