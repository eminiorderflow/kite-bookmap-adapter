package com.bookmap.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Depth;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.Tick;
import com.zerodhatech.models.User;
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
import java.util.*;
import java.util.concurrent.*;

@Layer0LiveModule(fullName = "Zerodha Kite Realtime Provider")
public class RealTimeProvider extends ExternalLiveBaseProvider {

    public static final String API_KEY = null;
    public static final String API_SECRET = null;
    private static final int DEPTH_LEVELS_COUNT = 5;
    protected final HashMap<String, Instruments> aliasInstruments;
    protected Thread connectionThread = null;

    public KiteTicker connector;
    protected final ExecutorService singleThreadExecutor;
    protected final ScheduledExecutorService singleThreadScheduledExecutor;
    protected ObjectMapper objectMapper;
    protected CopyOnWriteArrayList<SubscribeInfo> knownInstruments = new CopyOnWriteArrayList<>();
    public static Map<String, Instrument> genericInstruments = new HashMap<>();
    public final String exchange;
    public final String wsPortNumber;
    public final String wsLink;

    public static ArrayList<Long> subscribeTokens = new ArrayList<Long>();
    public static ArrayList<Long> unsubscribeTokens = new ArrayList<Long>();

    public static final KiteConnect kiteConnect = new KiteConnect(API_KEY);
    public static final KiteTicker tickerProvider = new KiteTicker(kiteConnect.getAccessToken(), kiteConnect.getApiKey());
    public Instrument instrument = new Instrument();

    protected static class Instruments {
        protected final String alias;
        protected final double pips;

        public Instruments(String alias, double pips) {
            this.alias = alias;
            this.pips = pips;
        }
    }

    ;

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

    public RealTimeProvider(String exchange, String wsPortNumber, String wsLink) {
        aliasInstruments = new HashMap<>();
        singleThreadExecutor = Executors.newSingleThreadExecutor();
        singleThreadScheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.exchange = exchange;
        this.wsPortNumber = wsPortNumber;
        this.wsLink = wsLink;
        getInstruments();
    }

    public void listenForOrderBookL2(Tick ticks) {
        ArrayList<Depth> bidDepthTick = ticks.getMarketDepth().get("buy");
        ArrayList<Depth> askDepthTick = ticks.getMarketDepth().get("sell");
        for (int i = 0; i < DEPTH_LEVELS_COUNT; i++) {
            for (Layer1ApiDataListener listener : dataListeners) {
                listener.onDepth(String.valueOf(ticks.getInstrumentToken()), true,
                        (int) (bidDepthTick.get(i).getPrice() * instrument.getTick_size()),
                        bidDepthTick.get(i).getQuantity());
                listener.onDepth(String.valueOf(ticks.getInstrumentToken()), false,
                        (int) (askDepthTick.get(i).getPrice() * instrument.getTick_size()),
                        askDepthTick.get(i).getQuantity());
            }
        }
    }

    public void onTradeRecord(Tick tradeRecord) {
        boolean isBidAggressor = tradeRecord.getChange() < 0;
        boolean isOtc = false;

        int size = (int) tradeRecord.getLastTradedQuantity();

        if (isBidAggressor) {
            dataListeners.forEach(l -> l.onTrade(String.valueOf(tradeRecord.getInstrumentToken()),
                    Math.floor(tradeRecord.getLastTradedPrice()),
                    size, new TradeInfo(isOtc, isBidAggressor)));
        } else {
            dataListeners.forEach(l -> l.onTrade(String.valueOf(tradeRecord.getInstrumentToken()),
                    Math.ceil(tradeRecord.getLastTradedPrice()),
                    size, new TradeInfo(isOtc, isBidAggressor)));

        }
    }

    @Override
    public void unsubscribe(String alias) {
        synchronized (aliasInstruments) {
            Long aliasToken = Long.valueOf(alias);
            unsubscribeTokens.add(aliasToken);
            subscribeTokens.remove(aliasToken);
            tickerProvider.unsubscribe(unsubscribeTokens);
            if (aliasInstruments.remove(alias) != null) {
                instrumentListeners.forEach(l -> l.onInstrumentRemoved(alias));
            }
        }
    }

    @Override
    public String formatPrice(String alias, double price) {
        double pips = instrument.getTick_size();
        return formatPriceDefault(pips, price);
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

    private void handleLogin(UserPasswordLoginData userPasswordLoginData) throws KiteException, IOException {
        String user = userPasswordLoginData.user.trim();
        String password = userPasswordLoginData.password.trim();
        if (!user.isEmpty() || !password.isEmpty()) {
            adminListeners.forEach(l -> l.onLoginFailed(
                    LoginFailedReason.WRONG_CREDENTIALS,
                    "You have entered credentials. "
                            + "Note that if you want to trade\nyou should connect using trading provider instead of demo.\n"
                            + "If you don't, clear your credentials please.")
            );
            return;
        }
        kiteConnect.setUserId(userPasswordLoginData.user.trim());
        User newUser = kiteConnect.generateSession(userPasswordLoginData.password.trim(), API_SECRET);
        kiteConnect.setAccessToken(newUser.accessToken);
        kiteConnect.setPublicToken(newUser.publicToken);
        getConnector().connect();
        adminListeners.forEach(Layer1ApiAdminListener::onLoginSuccessful);
    }

    @Override
    public String getSource() {
        return "Zerodha Kite";
    }

    @Override
    public void close() {
        try {
            Log.info("Closing connector");
            if (connector != null) {
                connector.disconnect();
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

    public KiteTicker getConnector() {
        boolean isConnected = tickerProvider.isConnectionOpen();
        if (!isConnected) {
            connector = new KiteTicker(kiteConnect.getAccessToken(), kiteConnect.getApiKey());
        }
        return connector;
    }

    ;

    public KiteTicker getNewConnector() {
        connector = new KiteTicker(kiteConnect.getAccessToken(), kiteConnect.getApiKey());
        return connector;
    }

    private static String createAlias(String symbol, String exchange, String type) {
        return symbol + "/" + exchange + "/" + type;
    }

    protected final HashMap<String, Instruments> insts = new HashMap<>();

    @Override
    public void subscribe(SubscribeInfo subscribeInfo) {
        String symbol = subscribeInfo.symbol;
        String exchange = subscribeInfo.exchange;
        String type = subscribeInfo.type;
        String alias = createAlias(symbol, exchange, type);
        Long symbolToken = Long.valueOf(symbol);
        subscribeTokens.add(symbolToken);
        synchronized (insts) {
            if (insts.containsKey(alias)) {
                instrumentListeners.forEach(l -> l.onInstrumentAlreadySubscribed(symbol, exchange, type));
            } else {
                double pips = instrument.getTick_size();
                final Instruments newInstruments = new Instruments(alias, pips);
                insts.put(alias, newInstruments);

                final InstrumentInfo instrumentInfo = new InstrumentInfo(
                        symbol, exchange, type, newInstruments.pips, 1, "", false);

                tickerProvider.subscribe(subscribeTokens);
                tickerProvider.setMode(subscribeTokens, KiteTicker.modeFull);
                instrumentListeners.forEach(l -> l.onInstrumentAdded(alias, instrumentInfo));
            }
        }
    }

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
            double basicTickSize = generic.getTick_size();
            List<Double> options = new ArrayList<>();
            options.add((double) basicTickSize);
            options.add((double) 5 * basicTickSize);
            options.add((double) 10 * basicTickSize);
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
