package com.bookmap.client;

import com.zerodhatech.models.MarketDepth;

public interface MarketDepthListener {
    public void onMarketDepth(String symbol, MarketDepth marketDepth);
}

