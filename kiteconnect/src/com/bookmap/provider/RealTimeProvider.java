package com.bookmap.provider;

import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Depth;
import com.zerodhatech.models.Tick;
import com.zerodhatech.ticker.KiteTicker;

import lombok.SneakyThrows;
import lombok.Data;
import velox.api.layer0.annotations.Layer0LiveModule;
import velox.api.layer0.live.ExternalLiveBaseProvider;
import velox.api.layer1.Layer1ApiAdminListener;
import velox.api.layer1.Layer1ApiDataListener;
import velox.api.layer1.common.Log;
import velox.api.layer1.data.DefaultAndList;
import velox.api.layer1.data.InstrumentInfo;
import velox.api.layer1.data.Layer1ApiProviderSupportedFeatures;
import velox.api.layer1.data.Layer1ApiProviderSupportedFeaturesBuilder;
import velox.api.layer1.data.LoginData;
import velox.api.layer1.data.LoginFailedReason;
import velox.api.layer1.data.OrderSendParameters;
import velox.api.layer1.data.OrderUpdateParameters;
import velox.api.layer1.data.UserPasswordLoginData;
import velox.api.layer1.data.SubscribeInfo;
import velox.api.layer1.data.TradeInfo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;


@Layer0LiveModule(fullName = "Zerodha Kite Realtime Provider")
public class RealTimeProvider extends ExternalLiveBaseProvider {

    public static final int DEPTH_LEVELS_COUNT = 5;
    public static final double TICK_SIZE = 0.05;
    public static final int CONTRACT_SIZE = 20;

    protected Thread connectionThread = null;

    private static KiteTicker tickerProvider;
    private static double prevTickPrice;

    protected CopyOnWriteArrayList<SubscribeInfo> knownInstruments = new CopyOnWriteArrayList<>();
    public static Map<String, Instrument> genericInstruments = new HashMap<>();
    public static ArrayList<Long> subscribeTokens = new ArrayList<Long>();
    public static ArrayList<Long> unsubscribeTokens = new ArrayList<Long>();

    protected static class Instrument {
    protected final String symbol;
    protected final double pips;

    public Instrument(String symbol, double pips) {
        this.symbol = symbol;
        this.pips = pips;
    }};

    protected final HashMap<String, Instrument> instrument = new HashMap<>();

    public static void setTickerProvider(KiteTicker newTickerProvider){
        tickerProvider = newTickerProvider;
    }

    public static KiteTicker getTickerProvider(){
        return tickerProvider;
    }

    public void setPrevTickPrice(double newPrevTickPrice){
        prevTickPrice = newPrevTickPrice;
    }

    public double getPrevTickPrice(){
        return prevTickPrice;
    }

    @Data
    public static class MarketDataComponent {
        public MarketDataComponent(String alias, long id, int price, int size, boolean isBid) {
            super();
            this.alias = alias;
            this.id = id;
            this.price = price;
            this.size = size;
            this.isBid = isBid;
        }

        String alias;
        long id;
        int price;
        int size;
        boolean isBid;
    }

    @Data
    protected static class StatusInfoLocal {
        String instrumentAlias;
        double unrealizedPnl;
        double realizedPnl;
        String currency;
        int position;
        double averagePrice;
        int volume;
        int workingBuys;
        int workingSells;
    }

    public void listenForOrderBookL2() {
        Log.info("Is connection open? " + getTickerProvider().isConnectionOpen());
        getTickerProvider().setOnTickerArrivalListener(ticks -> {
            Log.info("Ticks" + ticks);
            ArrayList<Depth> bidDepthTick = ticks.get(0).getMarketDepth().get("buy");
            ArrayList<Depth> askDepthTick = ticks.get(0).getMarketDepth().get("sell");
            for (int i = 0; i < DEPTH_LEVELS_COUNT; i++) {
                for (Layer1ApiDataListener listener : dataListeners) {
                    listener.onDepth(String.valueOf(ticks.get(0).getInstrumentToken()), true,
                            (int) (bidDepthTick.get(i).getPrice() / TICK_SIZE),
                            bidDepthTick.get(i).getQuantity());
                    listener.onDepth(String.valueOf(ticks.get(0).getInstrumentToken()), false,
                            (int) (askDepthTick.get(i).getPrice() / TICK_SIZE),
                            askDepthTick.get(i).getQuantity());
                }
            }
            onTradeRecord(ticks);
        });
    }

    public void onTradeRecord(ArrayList<Tick> tradeRecord) throws NullPointerException{

        boolean isBidAggressor = tradeRecord.get(0).getLastTradedPrice() >= getPrevTickPrice();

        boolean isOtc = false;

        int size = (int) tradeRecord.get(0).getLastTradedQuantity();

        if (isBidAggressor) {
            dataListeners.forEach(l -> l.onTrade(String.valueOf(tradeRecord.get(0).getInstrumentToken()),
                    tradeRecord.get(0).getLastTradedPrice() / TICK_SIZE,
                    size, new TradeInfo(isOtc, isBidAggressor)));
        } else {
            dataListeners.forEach(l -> l.onTrade(String.valueOf(tradeRecord.get(0).getInstrumentToken()),
                    tradeRecord.get(0).getLastTradedPrice() / TICK_SIZE,
                    size, new TradeInfo(isOtc, isBidAggressor)));

        }
        setPrevTickPrice(tradeRecord.get(0).getLastTradedPrice());
//        prevTickPrice = tradeRecord.get(0).getLastTradedPrice();
    }

    @Override
    public String formatPrice(String alias, double price) {
        return formatPriceDefault(TICK_SIZE, price);
    }

    @Override
    public void sendOrder(OrderSendParameters orderSendParameters) {
        throw new RuntimeException("Not trading capable");
    }

    @Override
    public void updateOrder(OrderUpdateParameters orderUpdateParameters) {
        throw new RuntimeException("Not trading capable");
    }

    @SneakyThrows
    @Override
    public void login(LoginData loginData) {
        UserPasswordLoginData userPasswordLoginData = (UserPasswordLoginData) loginData;
        connectionThread = new Thread(() -> {
            try {
                handleLogin(userPasswordLoginData);
            } catch (KiteException | IOException e) {
                e.printStackTrace();
            }
        });
        connectionThread.start();
    }

    public void handleLogin(UserPasswordLoginData userPasswordLoginData) throws KiteException, IOException {
        String apiKey = userPasswordLoginData.user.trim();
        String accessToken = userPasswordLoginData.password.trim();
        if (apiKey.isEmpty() || accessToken.isEmpty()) {
            loginFailed();
            return;
        }
        KiteTicker newKickerProvider = new KiteTicker(accessToken, apiKey);
        setTickerProvider(newKickerProvider);
        getTickerProvider().connect();
        boolean isConnected = getTickerProvider().isConnectionOpen();
        if (!isConnected){
            loginFailed();
            return;
        }
        adminListeners.forEach(Layer1ApiAdminListener::onLoginSuccessful);
    }

    public void loginFailed() {
        adminListeners.forEach(l -> l.onLoginFailed(
                LoginFailedReason.WRONG_CREDENTIALS,
                "Please provide username and password. You may need to re-generate the access token.")
        );
    }

    @Override
    public void subscribe(SubscribeInfo subscribeInfo) {
        String symbol = subscribeInfo.symbol.toString();
        String exchange = subscribeInfo.exchange;
        String type = subscribeInfo.type;
//        String alias = createAlias(symbol, exchange, type);
        Long symbolToken = Long.valueOf(symbol);
        subscribeTokens.add(symbolToken);
        synchronized (instrument) {
            if (instrument.containsKey(symbol)) {
                instrumentListeners.forEach(l -> l.onInstrumentAlreadySubscribed(symbol, exchange, type));
            } else {
                final Instrument newInstrument = new Instrument(symbol, TICK_SIZE);
                instrument.put(symbol, newInstrument);

                final InstrumentInfo instrumentInfo = new InstrumentInfo(
                        newInstrument.symbol,
                        exchange,
                        type,
                        newInstrument.pips,
                        newInstrument.pips * CONTRACT_SIZE,
                        newInstrument.symbol,
                        true);
                getTickerProvider().subscribe(subscribeTokens);
                getTickerProvider().setMode(subscribeTokens, KiteTicker.modeFull);
                instrumentListeners.forEach(l -> l.onInstrumentAdded(symbol, instrumentInfo));
                Log.info("Subscribed to " + symbol);
                listenForOrderBookL2();
            }
        }
    }

    @Override
    public void unsubscribe(String alias) {
        synchronized (instrument) {
            Long aliasToken = Long.valueOf(alias);
            unsubscribeTokens.add(aliasToken);
            subscribeTokens.remove(aliasToken);
            getTickerProvider().unsubscribe(unsubscribeTokens);
            if (instrument.remove(alias) != null) {
                instrumentListeners.forEach(l -> l.onInstrumentRemoved(alias));
            }
        }
    }

    @Override
    public String getSource() {
        return "Zerodha Kite";
    }

    @Override
    public void close() {
        try {
            Log.info("Closing connector");
            if (getTickerProvider() != null) {
                getTickerProvider().disconnect();
            }
        } catch (Exception ex) {
            Log.error("Unable to close connector", ex);
        }
    }

    protected void onConnectionRestored() {
    }

    public enum ClosedConnectionType {
        Disconnect, ConnectionFailure;
    }

//    private static String createAlias(String symbol, String exchange, String type) {
//        return symbol + "/" + exchange + "/" + type;
//    }

    @Override
    public Layer1ApiProviderSupportedFeatures getSupportedFeatures() {
        Layer1ApiProviderSupportedFeaturesBuilder a = super.getSupportedFeatures().toBuilder();
        a.setExchangeUsedForSubscription(false);
        a.setKnownInstruments(knownInstruments);
        a.setPipsFunction(s -> {
            String alias = s.type + "@" + s.symbol;

            if (!genericInstruments.containsKey(alias)) {
                return null;
            }

            Instrument generic = genericInstruments.get(alias);
            double basicTickSize = TICK_SIZE;
            List<Double> options = new ArrayList<>();
            options.add((double) basicTickSize);
            options.add((double) 5 * basicTickSize);
            options.add((double) 10 * basicTickSize);
            options.add((double) 20 * basicTickSize);
            options.add((double) 50 * basicTickSize);
            options.add((double) 100 * basicTickSize);
            options.add((double) 500 * basicTickSize);
            options.add((double) 1_000 * basicTickSize);
            options.add((double) 5_000 * basicTickSize);
            options.add((double) 10_000 * basicTickSize);
            return new DefaultAndList<Double>(basicTickSize, options);
        });
        return a.build();
    }

    protected void getInstruments() {
    }
}
