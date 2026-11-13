package com.example.finance.trader.publication;

import com.example.finance.trader.application.EventMetadata;
import com.example.finance.trader.application.StoredTrade;

public interface TradeEventPublisher {
    EventMetadata publish(StoredTrade trade) throws Exception;
}
