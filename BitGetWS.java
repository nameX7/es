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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

public class BitGetWS extends WebSocketClient {
    private static final Logger log = LoggerFactory.getLogger(BitGetWS.class);
    private final Map<String, PositionEvent> events = new ConcurrentHashMap<>();
    private final Map<String, OrderEvent> orders = new ConcurrentHashMap<>();

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

        // Добавляем таймаут подключения
        this.setConnectionLostTimeout(60);
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
        if (isReconnecting) {
            return; // Already in the process of reconnecting
        }
        isReconnecting = true;

        Thread.startVirtualThread(() -> {
            long delay = 1000; // Start with 1 second
            final long maxDelay = 60000; // Max delay of 60 seconds

            while (true) {
                try {
                    log.info("Attempting to reconnect to BitGet WebSocket... (Next attempt in {}ms)", delay);
                    if (reconnectBlocking()) {
                        log.info("Reconnection to BitGet WebSocket successful!");
                        // isConnected and isAuthenticated will be set in onOpen and handleEventMessage
                        return; // Exit the loop on success
                    }
                } catch (Exception e) {
                    log.warn("reconnectBlocking() failed for BitGet WebSocket", e);
                }

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Reconnect thread was interrupted.", e);
                    return;
                }

                // Exponential backoff
                delay = Math.min(delay * 2, maxDelay);
            }
        });
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
        log.debug("Checking for position open event in data: {}", object);

        if (!object.has("data") || !object.get("data").isJsonArray() || object.getAsJsonArray("data").isEmpty()) {
            return; // No data, nothing to process
        }
        JsonArray data = object.getAsJsonArray("data");
        JsonObject orderJson = data.get(0).getAsJsonObject();

        try {
            if (!orderJson.has("status") || !orderJson.has("tS") || !orderJson.has("instId")) {
                log.warn("Position data is missing critical fields ('status', 'tS', 'instId'). Cannot determine position state.");
                return;
            }

            String status = orderJson.get("status").getAsString();
            String tSide = orderJson.get("tS").getAsString(); // e.g., "open_long", "close_short"

            // Позиция считается открытой только при полном исполнении ордера на открытие
            boolean isFill = status.toLowerCase().contains("fill");
            boolean isOpen = tSide.toLowerCase().startsWith("open");

            if (isFill && isOpen) {
                String pair = orderJson.get("instId").getAsString().replace("_UMCBL", "");
                log.info("Position open condition met for pair '{}'. Status: {}, Trade Side: {}", pair, status, tSide);

                for (String s : events.keySet()) {
                    if (s.equalsIgnoreCase(pair)) {
                        PositionEvent event = events.get(s);
                        // Запрашиваем актуальную информацию о позиции через REST API для надежности
                        ts.getPositions(user).stream()
                                .filter(p -> p.getSymbol().equalsIgnoreCase(pair))
                                .findFirst() // Используем findFirst для безопасности
                                .ifPresent(event::onPositionOpened);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to process position notification. Raw data: {}", orderJson, e);
        }
    }

    public void addOrderListener(String pair, OrderEvent event) {
        orders.put(pair, event);
    }

    private void notifyOrderListeners(JsonObject object) {
        log.info("Received order event data: {}", object);

        if (!object.has("data") || !object.get("data").isJsonArray() || object.getAsJsonArray("data").isEmpty()) {
            log.warn("Order event is missing 'data' array or it is empty.");
            return;
        }
        JsonArray data = object.getAsJsonArray("data");
        JsonObject orderJson = data.get(0).getAsJsonObject();

        try {
            // --- Безопасное извлечение и проверка обязательных полей ---
            if (!orderJson.has("instId") || !orderJson.has("status") || !orderJson.has("ordId") || !orderJson.has("tS")) {
                log.error("Critical order data is missing in the JSON object: {}", orderJson);
                return;
            }

            String pair = orderJson.get("instId").getAsString().replace("_UMCBL", "");
            String status = orderJson.get("status").getAsString();
            String id = orderJson.get("ordId").getAsString();
            String tradeInfo = orderJson.get("tS").getAsString();

            // --- Безопасный парсинг tradeInfo ---
            String side, posSide;
            if (tradeInfo.contains("_")) {
                side = tradeInfo.substring(0, tradeInfo.indexOf('_'));
                posSide = tradeInfo.substring(tradeInfo.indexOf('_') + 1);
            } else {
                log.warn("Cannot determine trade side and position side from 'tS' field: {}", tradeInfo);
                side = "unknown";
                posSide = "unknown";
            }

            boolean isFill = status.equals("full-fill") || status.equals("triggered");
            log.info("Parsed order data: pair: {}, status: {}, id: {}, side: {}, isFill: {}", pair, status, id, side, isFill);

            // --- Создание и заполнение объекта Order с проверками ---
            Order order = new Order();
            order.setOrderId(id);
            order.setSymbol(pair);
            order.setState(status);
            order.setTradeSide(side);
            order.setMerginCoin("USDT"); // Assuming USDT
            order.setStopTraling(false); // Default value

            // --- Безопасное извлечение и преобразование опциональных полей ---
            if (orderJson.has("px") && !orderJson.get("px").isJsonNull()) {
                order.setPrice(new BigDecimal(orderJson.get("px").getAsString()));
            }
            if (orderJson.has("sz") && !orderJson.get("sz").isJsonNull()) {
                order.setSize(new BigDecimal(orderJson.get("sz").getAsString()));
            }
            if (orderJson.has("ordType") && !orderJson.get("ordType").isJsonNull()) {
                order.setOrderType(orderJson.get("ordType").getAsString());
            }
            if (orderJson.has("posSide") && !orderJson.get("posSide").isJsonNull()) {
                String ps = orderJson.get("posSide").getAsString().toLowerCase();
                order.setPosSide(ps.equals("buy") || ps.equals("long") ? "LONG" : "SHORT");
            }
            if (orderJson.has("side") && !orderJson.get("side").isJsonNull()) {
                order.setSide(orderJson.get("side").getAsString());
            }
            if (orderJson.has("clOrdId") && !orderJson.get("clOrdId").isJsonNull()) {
                order.setClient0Id(orderJson.get("clOrdId").getAsString());
            }
            if (orderJson.has("fillSz") && !orderJson.get("fillSz").isJsonNull()) {
                order.setFilledAmount(new BigDecimal(orderJson.get("fillSz").getAsString()));
            }
            if (orderJson.has("eps") && !orderJson.get("eps").isJsonNull()) {
                order.setOrderSource(orderJson.get("eps").getAsString());
            }
            if (orderJson.has("tdMode") && !orderJson.get("tdMode").isJsonNull()) {
                order.setMarginMode(orderJson.get("tdMode").getAsString());
            }
            if (orderJson.has("lever") && !orderJson.get("lever").isJsonNull()) {
                order.setLeverage(Integer.parseInt(orderJson.get("lever").getAsString()));
            }
            if (orderJson.has("hM") && !orderJson.get("hM").isJsonNull()) {
                order.setHoldMode(orderJson.get("hM").getAsString());
            }

            // --- Уведомление слушателей ---
            if (isFill) {
                for (String s : orders.keySet()) {
                    if (s.equalsIgnoreCase(pair)) {
                        OrderEvent event = orders.get(s);
                        event.onOrder(order);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to process order notification. Raw data: {}", orderJson, e);
        }
    }



    private void sendPong() {
        if (isAuthenticated && isConnected) {
            send("pong");
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