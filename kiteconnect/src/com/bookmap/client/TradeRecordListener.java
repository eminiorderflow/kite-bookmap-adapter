package com.bookmap.client;

import com.zerodhatech.models.Tick;

public interface TradeRecordListener {
    public void onTradeRecord(String symbol, Tick tradeRecord);
}

