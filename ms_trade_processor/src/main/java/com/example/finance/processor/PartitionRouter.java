package com.example.finance.processor;

public class PartitionRouter {
    public int route(String side, String symbol) {
        if (!"BUY".equals(side) && !"SELL".equals(side)) throw new InvalidTradeException("Invalid side");
        return ("BUY".equals(side) ? 0 : 3) + Math.floorMod(symbol.hashCode(), 3);
    }
}
