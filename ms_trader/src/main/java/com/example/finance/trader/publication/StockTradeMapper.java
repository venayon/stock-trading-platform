package com.example.finance.trader.publication;

import com.example.finance.avro.Side;
import com.example.finance.avro.StockTrade;
import com.example.finance.trader.application.StoredTrade;

public class StockTradeMapper {
    public StockTrade map(StoredTrade stored) {
        var trade = stored.trade();
        return StockTrade.newBuilder().setTradeId(stored.tradeId()).setTradeDate(trade.tradeDate())
            .setSide(Side.valueOf(trade.side())).setSymbol(trade.symbol()).setQuantity(trade.quantity())
            .setPrice(trade.price()).setCurrency(trade.currency()).setCounterparty(trade.counterparty()).build();
    }
}
