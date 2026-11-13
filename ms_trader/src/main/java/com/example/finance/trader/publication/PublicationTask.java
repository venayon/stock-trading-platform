package com.example.finance.trader.publication;

import com.example.finance.trader.application.StoredTrade;

public record PublicationTask(StoredTrade trade, String leaseToken, int attempt) {
}
