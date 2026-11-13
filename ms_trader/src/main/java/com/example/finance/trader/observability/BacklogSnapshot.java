package com.example.finance.trader.observability;

import java.time.Instant;

public record BacklogSnapshot(long pending, Instant oldest) {
}
