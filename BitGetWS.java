package com.plovdev.bot.modules.beerjes.monitoring;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.plovdev.bot.listeners.OnOrder;
import com.plovdev.bot.listeners.OrderEvent;
import com.plovdev.bot.listeners.PositionEvent;
import com.plovdev.bot.modules.beerjes.BitGetTradeService;
import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.security.BitGetSecurity;
import com.plovdev.bot.modules.databases.SignalDB;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.models.SettingsService;
import com.plovdev.bot.modules.models.TypeValueSwitcher;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;
import java.util.*;

public class BitGetWS extends WebSocketClient {
    private static final Logger log = LoggerFactory.getLogger(BitGetWS.class);
    private final Map<String, PositionEvent> events = new HashMap<>();
    private final Map<String, OrderEvent> orders = new HashMap<>();

    private final Gson gson = new Gson();
    private final String apiKey;
    private final String secretKey;
    private final String passphrase;
    private final BitGetSecurity security;
    private boolean needToRestart = false;
    private String stopId;

    public Map<String, PositionEvent> getEvents() {
        return events;
    }

    public Map<String, OrderEvent> getOrders() {
        return orders;
    }

    public Gson getGson() {
        return gson;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getPassphrase() {
        return passphrase;
    }

    public BitGetSecurity getSecurity() {
        return security;
    }

    public boolean isNeedToRestart() {
        return needToRestart;
    }

    public void setNeedToRestart(boolean needToRestart) {
        this.needToRestart = needToRestart;
    }

    public String getStopId() {
        return stopId;
    }

    public void setStopId(String stopId) {
        this.stopId = stopId;
    }

    public Set<String> getActiveSignals() {
        return activeSignals;
    }

    public SignalDB getSignalDB() {
        return signalDB;
    }

    public Object getConnectionLock() {
        return connectionLock;
    }

    public SettingsService getSettings() {
        return settings;
    }

    public TypeValueSwitcher<Boolean> getIsStopTraling() {
        return isStopTraling;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    public boolean isAuthenticated() {
        return isAuthenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        isAuthenticated = authenticated;
    }

    public boolean isReconnecting() {
        return isReconnecting;
    }

    public void setReconnecting(boolean reconnecting) {
        isReconnecting = reconnecting;
    }

    public OrderResult getOrderResult() {
        return orderResult;
    }

    public void setOrderResult(OrderResult orderResult) {
        this.orderResult = orderResult;
    }

    public BitGetPositionMonitor getMonitor() {
        return monitor;
    }

    public BitGetTradeService getTs() {
        return ts;
    }

    public UserEntity getUser() {
        return user;
    }

    private final Set<String> activeSignals = new HashSet<>();
    private final SignalDB signalDB = new SignalDB();
    private final Object connectionLock = new Object();
    private final SettingsService settings = new SettingsService();
    private final TypeValueSwitcher<Boolean> isStopTraling = new TypeValueSwitcher<>(false);


    public OnOrder getOnOrder() {
        return onOrder;
    }

    public void setOnOrder(OnOrder onOrder) {
        this.onOrder = onOrder;
    }

    private OnOrder onOrder = OrderResult::no;

    private boolean isConnected = false;
    private boolean isAuthenticated = false;
    private boolean isReconnecting = false;

    private OrderResult orderResult = OrderResult.no();

    private final BitGetPositionMonitor monitor;
    private final BitGetTradeService ts;

    private final UserEntity user;

    public BitGetWS(UserEntity user, BitGetSecurity security, BitGetTradeService tradeService) {
        super(URI.create("wss://ws.bitget.com/mix/v1/stream"));

        this.apiKey = security.decrypt(user.getApiKey());
        this.secretKey = security.decrypt(user.getSecretKey());
        this.passphrase = security.decrypt(user.getPhrase());
        this.security = security;
        ts = tradeService;
        monitor = new BitGetPositionMonitor(tradeService);

        this.user = user;

        Thread.startVirtualThread(() -> {
            Timer ping = new Timer();
            ping.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    sendPong();
                }
            }, 5000, 30000);
        });

        // Добавляем таймаут подключения
        this.setConnectionLostTimeout(36);
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        log.info("Connected to BitGet Futures WebSocket");
        synchronized (connectionLock) {
            isConnected = true;
            connectionLock.notifyAll();
        }
        authenticate();
    }

    @Override
    public void onMessage(String message) {
        if (!message.equals("pong")) {
            try {
                JsonObject json = gson.fromJson(message, JsonObject.class);

                if (json.has("event")) {
                    handleEventMessage(json);
                } else if (json.has("action")) {
                    handleActionMessage(json);
                } else if (json.has("data")) {
                    handleDataMessage(json);
                }
            } catch (Exception e) {
                log.error("Error processing message: ", e);
            }
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        log.warn("Connection closed: {} (code: {})", reason, code);

        synchronized (connectionLock) {
            isConnected = false;
            isAuthenticated = false;
            connectionLock.notifyAll();
        }

        // Автопереподключение (если не мы сами закрыли)
        if (code != 1000 && !isReconnecting) {
            log.info("Try reconnect");
            reconnect();
        }
    }

    @Override
    public void onError(Exception ex) {
        log.error("WebSocket error: ", ex);
    }

    /**
     * Ожидание подключения к WebSocket
     */
    public void waitForConnection() throws InterruptedException {
        log.debug("Try connecting to BitGet WebSocket");
        synchronized (connectionLock) {
            long startTime = System.currentTimeMillis();
            while (!isConnected) {
                connectionLock.wait(1000);
                if (System.currentTimeMillis() - startTime > 10000) { // 10 секунд таймаут
                    throw new RuntimeException("WebSocket connection timeout");
                }
            }
        }
    }

    public OrderResult callbackReady() {
        return orderResult;
    }

    /**
     * Ожидание успешной аутентификации
     */
    public void waitForAuthentication() throws InterruptedException {
        log.debug("Wait auth result");
        synchronized (connectionLock) {
            long startTime = System.currentTimeMillis();
            while (!isAuthenticated) {
                connectionLock.wait(1000);
                if (System.currentTimeMillis() - startTime > 10000) { // 10 секунд таймаут
                    throw new RuntimeException("WebSocket authentication timeout");
                }
            }
        }
    }

    /**
     * Автопереподключение
     */
    @Override
    public void reconnect() {
        new Thread(() -> {
            isReconnecting = true;
            try {
                System.out.println("Attempting to reconnect...");

                if (reconnectBlocking()) {
                    log.info("Reconnection successful");
                    synchronized (connectionLock) {
                        isConnected = true;
                        connectionLock.notifyAll();
                    }


                    // Повторная аутентификация
                    authenticate();
                    waitForAuthentication();
                    sendPong();

                    subscribeToChannels();
                }
            } catch (Exception e) {
                log.error("Reconnection failed: ", e);
            } finally {
                isReconnecting = false;
            }
        }).start();
    }

    private void authenticate() {
        try {
            log.info("Try auth in BitGet WebSocket");
            long timestamp = System.currentTimeMillis();
            String sign = security.generateSignature(timestamp + "GET" + "/user/verify", secretKey);

            JsonObject authArgs = new JsonObject();
            authArgs.addProperty("apiKey", apiKey);
            authArgs.addProperty("passphrase", passphrase);
            authArgs.addProperty("timestamp", timestamp);
            authArgs.addProperty("sign", sign);

            JsonArray argsArray = new JsonArray();
            argsArray.add(authArgs);

            JsonObject authMessage = new JsonObject();
            authMessage.addProperty("op", "login");
            authMessage.add("args", argsArray);

            send(gson.toJson(authMessage));
        } catch (Exception e) {
            System.err.println("Authentication error: " + e.getMessage());
        }
    }

    private void handleEventMessage(JsonObject json) {
        String event = json.get("event").getAsString();
        if ("login".equals(event)) {
            if ("0".equals(json.get("code").getAsString())) {
                synchronized (connectionLock) {
                    isAuthenticated = true;
                    connectionLock.notifyAll();
                }
                System.out.println("Authenticated successfully");

                // После аутентификации подписываемся на нужные каналы
                subscribeToChannels();
                orderResult = onOrder.onReady();
            } else {
                System.err.println("Authentication failed: " + json.get("msg").getAsString());
            }
        }
    }

    private void handleActionMessage(JsonObject json) {
        String action = json.get("action").getAsString();
        if ("ping".equals(action)) {
            sendPong();
        } else if (action.equals("snapshot") || action.equals("update")) {
            handleDataMessage(json);
        }
    }

    private void handleDataMessage(JsonObject json) {
        if (!json.has("arg")) return;

        JsonObject arg = json.getAsJsonObject("arg");
        String channel = arg.get("channel").getAsString();
        String symbol = arg.get("instId").getAsString();

        if ("orders".equals(channel)) {
            try {
                notifyOrderListeners(json);
                notifyPositionListeners(json);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } else if (channel.equals("ticker")) {
            handleTickerUpdate(json, symbol);
        }
    }

    private void handleTickerUpdate(JsonObject json, String symbol) {
        JsonArray data = json.getAsJsonArray("data");
        if (data == null || data.isEmpty()) return;

        JsonObject ticker = data.get(0).getAsJsonObject();
        String price = ticker.get("last").getAsString(); // Используем last price для трейлинга

        for (String pair : activeSignals) {
            if (pair.equals(symbol)) {
                BigDecimal current = new BigDecimal(price);
                if (!isStopTraling.getT()) {
                    //Position position = user.getUserBeerj().getPositions(user).stream().filter(p -> p.getSymbol().equals(symbol)).toList().getFirst();
                    //if (monitor.stopInProfit(user, position, current, stopId, new SymbolInfo()).succes()) {
                    //   isStopTraling.setT(true);
                    //}
                }
            }
        }
    }

    public void addPositionListener(String pair, PositionEvent event) {
        events.put(pair, event);
    }

    private void notifyPositionListeners(JsonObject object) {
        log.info("Get position event");
        JsonArray data = object.getAsJsonArray("data");
        JsonObject order = data.get(0).getAsJsonObject();

        String pair = order.get("instId").getAsString().replace("_UMCBL", "");
        String status = order.get("status").getAsString();
        String tSide = order.get("tS").getAsString();
        boolean isFeel = status.contains("fill") && tSide.contains("open");
        log.info("position data params: pair: {}, status: {}, tSide: {}, isFeel: {}", pair, status, tSide, isFeel);

        if (isFeel) {
            for (String s : events.keySet()) {
                if (s.equalsIgnoreCase(pair)) {
                    PositionEvent event = events.get(s);
                    event.onPositionOpened(ts.getPositions(user).stream().filter(p -> p.getSymbol().equalsIgnoreCase(pair)).toList().getFirst());
                }
            }
        }
    }

    public void addOrderListener(String pair, OrderEvent event) {
        orders.put(pair, event);
    }

    private void notifyOrderListeners(JsonObject object) {
        log.info("Get order event");
        JsonArray data = object.getAsJsonArray("data");
        JsonObject order = data.get(0).getAsJsonObject();

        String pair = order.get("instId").getAsString().replace("_UMCBL", "");
        String status = order.get("status").getAsString();
        String id = order.get("ordId").getAsString();
        String tradeSide = order.get("tS").getAsString();
        String side = tradeSide.substring(0, tradeSide.indexOf('_'));
        String posSide = tradeSide.substring(tradeSide.indexOf('_')+1);

        boolean isFeel = status.equals("full-fill") || status.equals("triggered");

        log.info("order data params: pair: {}, status: {}, id: {}, side: {}, isFeel: {}", pair, status, id, side, isFeel);
        Order input = new Order();
        input.setOrderId(id);
        input.setSymbol(pair);
        input.setPrice(new BigDecimal(order.get("px").getAsString()));
        input.setSize(new BigDecimal(order.get("sz").getAsString()));
        input.setOrderType(order.get("ordType").getAsString());
        input.setStopTraling(false);

        String ps = order.get("posSide").getAsString().toLowerCase();
        input.setPosSide(ps.equals("buy") || ps.equals("long")? "LONG" : "SHORT");
        input.setSide(order.get("side").getAsString());
        input.setClient0Id(order.get("clOrdId").getAsString());
        input.setFilledAmount(new BigDecimal(order.get("fillSz").getAsString()));
        input.setState(status);
        input.setOrderSource(order.get("eps").getAsString());
        input.setTradeSide(side);
        input.setMerginCoin("USDT");
        input.setMarginMode(order.get("tdMode").getAsString());
        input.setLeverage(Integer.parseInt(order.get("lever").getAsString()));
        input.setHoldMode(order.get("hM").getAsString());

        if (isFeel) {
            for (String s : orders.keySet()) {
                if (s.equalsIgnoreCase(pair)) {
                    OrderEvent event = orders.get(s);
                    event.onOrder(input);
                }
            }
        }
    }



    private void sendPong() {
        if (isAuthenticated && isConnected) {
            send("ping");
            needToRestart = true;
        }
    }

    private void subscribeToChannels() {
        log.info("Subscribing to chanels...");
        // Подписываемся на трейды для всех символов с активными сигналами
        for (String symbol : activeSignals) {
            subscribeToPosition(symbol);
            //subscribeToTicker(symbol);
        }
    }


    public void subscribeToPosition(String symbol) {
        subscribe("default", "orders", "UMCBL");
        subscribe("default", "ordersAlgo", "UMCBL");
    }

    public void subscribeToTicker(String symbol) {
        subscribe(symbol, "ticker", "MC");
    }

    private void subscribe(String symbol, String chanel, String type) {
        // Проверяем что подключены и аутентифицированы
        if (!isConnected || !isAuthenticated) {
            throw new IllegalStateException("WebSocket not ready for subscription");
        }

        try {
            JsonObject args = new JsonObject();
            args.addProperty("channel", chanel);
            args.addProperty("instType", type);
            args.addProperty("instId", symbol);

            JsonArray argsArray = new JsonArray();
            argsArray.add(args);

            JsonObject subscribe = new JsonObject();
            subscribe.addProperty("op", "subscribe");
            subscribe.add("args", argsArray);

            send(gson.toJson(subscribe));
            System.out.printf("Subscribed to '%s' for: %s\n", symbol, chanel);
        } catch (Exception e) {
            System.err.printf("Subscription to '%s' failed for %s: %s\n", symbol, chanel, e.getMessage());
        }
    }

    public void addSignal(String symbol) {
        activeSignals.add(symbol);
    }
}