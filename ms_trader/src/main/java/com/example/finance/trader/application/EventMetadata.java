package com.example.finance.trader.application;

public record EventMetadata(String topic, int partition, long offset) {
}
