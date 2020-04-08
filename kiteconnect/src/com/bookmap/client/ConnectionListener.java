package com.bookmap.client;

public interface ConnectionListener {
    public void onConnectionLost(String message);
    public void onConnectionRestored();
}

