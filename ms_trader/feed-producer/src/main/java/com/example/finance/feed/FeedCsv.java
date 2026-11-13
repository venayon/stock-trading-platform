package com.example.finance.feed;

import com.example.finance.trader.application.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;

import org.apache.commons.csv.*;
import org.springframework.batch.item.*;

/**
 * Bounded physical-line reader, with byte-offset checkpoints (no full-file buffering).
 */
public class FeedCsv implements ItemStreamReader<FeedCsv.Row> {
    public static final String HEADER = "tradeDate,side,symbol,quantity,price,currency,counterparty";
    private final Path path;
    private final int limit;
    private FileInputStream file;
    private InputStream input;
    private long offset, line;
    public FeedCsv(Path path, int limit) {
        this.path = path;
        this.limit = limit;
    }

    public static Row parse(long line, String raw) {
        try (CSVParser parser =
                 CSVParser.parse(raw, CSVFormat.DEFAULT.builder().setIgnoreEmptyLines(false).build())) {
            List<CSVRecord> records = parser.getRecords();
            if (records.size() != 1 || records.getFirst().size() != 7)
                return new Row(line, null, "INVALID_COLUMNS");
            CSVRecord r = records.getFirst();
            String price = r.get(4).trim();
            if (!price.matches("[+-]?[0-9]+(?:\\.[0-9]+)?")) return new Row(line, null, "INVALID_PRICE");
            CreateTradeCommand command =
                new CreateTradeCommand(
                    LocalDate.parse(r.get(0).trim()),
                    r.get(1).trim(),
                    r.get(2).trim(),
                    Long.valueOf(r.get(3).trim()),
                    new BigDecimal(price),
                    r.get(5).trim().isEmpty() ? "USD" : r.get(5).trim(),
                    r.get(6));
            return new Row(line, new TradeValidator().validate(command), null);
        } catch (Exception e) {
            return new Row(line, null, "INVALID_TRADE");
        }
    }

    @Override
    public void open(ExecutionContext context) {
        try {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                throw new IOException("Feed must be a regular file");
            file = new FileInputStream(path.toFile());
            offset = context.getLong("offset", 0);
            line = context.getLong("line", 0);
            file.getChannel().position(offset);
            input = new BufferedInputStream(file, 65536);
            if (offset == 0) {
                String header = nextLine();
                if (header != null && header.startsWith("\uFEFF")) header = header.substring(1);
                if (!HEADER.equals(header)) throw new Rejected("INVALID_HEADER");
            }
        } catch (InvalidLine e) {
            throw new Rejected("INVALID_HEADER");
        } catch (IOException e) {
            throw new ItemStreamException("Cannot open feed", e);
        }
    }

    private String nextLine() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(Math.min(limit, 4096));
        boolean seen = false, large = false;
        int c;
        while ((c = input.read()) != -1) {
            seen = true;
            offset++;
            if (c == 10) break;
            if (bytes.size() < limit) bytes.write(c);
            else large = true;
        }
        if (!seen) return null;
        line++;
        if (large) throw new InvalidLine("RECORD_TOO_LARGE");
        byte[] raw = bytes.toByteArray();
        int size = raw.length;
        if (size > 0 && raw[size - 1] == 13) size--;
        try {
            return StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(raw, 0, size))
                .toString();
        } catch (CharacterCodingException e) {
            throw new InvalidLine("INVALID_UTF8");
        }
    }

    @Override
    public Row read() throws IOException {
        try {
            String raw = nextLine();
            return raw == null ? null : parse(line, raw);
        } catch (InvalidLine e) {
            return new Row(line, null, e.getMessage());
        }
    }

    @Override
    public void update(ExecutionContext context) {
        context.putLong("offset", offset);
        context.putLong("line", line);
    }

    @Override
    public void close() {
        try {
            if (input != null) input.close();
            if (file != null) file.close();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    public record Row(long line, CreateTradeCommand trade, String error) {
    }

    private static class InvalidLine extends IOException {
        InvalidLine(String message) {
            super(message);
        }
    }

    public static class Rejected extends RuntimeException {
        public Rejected(String code) {
            super(code);
        }
    }
}
