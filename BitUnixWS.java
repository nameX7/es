package com.plovdev.bot.modules.beerjes.monitoring;

import com.plovdev.bot.bots.EnvReader;
import com.plovdev.bot.listeners.OrderEvent;
import com.plovdev.bot.listeners.PositionEvent;
import com.plovdev.bot.modules.beerjes.BitUnixTradeService;
import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.Position;
import com.plovdev.bot.modules.beerjes.TradeService;
import com.plovdev.bot.modules.beerjes.bitunix.BitunixPrivateWsResponseListener;
import com.plovdev.bot.modules.beerjes.bitunix.FuturesWsPrivateClient;
import com.plovdev.bot.modules.beerjes.bitunix.OrderItem;
import com.plovdev.bot.modules.beerjes.security.BitUnixSecurity;
import com.plovdev.bot.modules.beerjes.utils.BitUnixUtils;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.SettingsService;
import com.plovdev.bot.modules.models.TypeValueSwitcher;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BitUnixWS {
    private final Map<String, PositionEvent> events = new HashMap<>();
    private final Map<String, OrderEvent> orders = new HashMap<>();
    private final TypeValueSwitcher<Boolean> isStopTrailing = new TypeValueSwitcher<>(false);
    private final SettingsService settings = new SettingsService();

    private String stopId;

    public Map<String, PositionEvent> getEvents() {
        return events;
    }

    public Map<String, OrderEvent> getOrders() {
        return orders;
    }

    public String getStopId() {
        return stopId;
    }

    public void setStopId(String stopId) {
        this.stopId = stopId;
    }

    public UserEntity getUser() {
        return user;
    }

    public BitUnixPositionMonitor getMonitor() {
        return monitor;
    }

    public BitUnixSecurity getSecurity() {
        return security;
    }

    private static final Logger log = LoggerFactory.getLogger(BitUnixWS.class);
    private final UserEntity user;
    private final BitUnixPositionMonitor monitor;
    private String symbol;
    private final BitUnixSecurity security = new BitUnixSecurity(EnvReader.getEnv("bitunixPassword"));

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public BitUnixWS(UserEntity user, BitUnixTradeService tradeService, String symbol) {
        this.user = user;
        monitor = new BitUnixPositionMonitor(tradeService);
        this.symbol = symbol;
    }

    public void addPositionListener(String symbol, PositionEvent event) {
        events.put(symbol, event);
    }

    public void addOrderListener(String symbol, OrderEvent event) {
        orders.put(symbol, event);
    }

    public void startMonitoring() {
        FuturesWsPrivateClient client = new FuturesWsPrivateClient(security.decrypt(user.getApiKey()), security.decrypt(user.getSecretKey()));
        client.connect(new BitunixPrivateWsResponseListener() {
            @Override
            public void onOrderChange(String orderResp) {
                checkOrderFill(orderResp);
            }
        });
        client.subscribe(symbol, "orders");

//        FuturesWsPublicClient futuresWsPublicClient = new FuturesWsPublicClient();
//        futuresWsPublicClient.connect(new BitunixPublicWsResponseListener() {
//            @Override
//            public void onPrice(PriceResp priceResp) {
//                handleTickerUpdate(priceResp);
//            }
//        });
//        futuresWsPublicClient.subPrice(new PriceSubReq(Collections.singletonList(new PriceSubArg(symbol))));
    }


    private void checkOrderFill(String resp) {
        JSONObject jsonObject = new JSONObject(resp);
        String data = jsonObject.getJSONObject("data").toString();
        OrderItem item = BitUnixUtils.parseInput(data);

        TradeService ts = user.getUserBeerj();
        boolean isFeel = item.getOrderStatus().toLowerCase().contains("fill");
        boolean isClose = item.isReduceOnly();
        log.info("position data params: pair: {}, status: {}, tSide: {}, isFeel: {}", symbol, item.getOrderStatus(), isClose ? "close" : "open", isFeel);

        if (isFeel && !isClose) {
            for (String s : events.keySet()) {
                if (s.equalsIgnoreCase(symbol)) {
                    PositionEvent event = events.get(s);
                    List<Position> positions = ts.getPositions(user).stream().filter(p -> p.getSymbol().equalsIgnoreCase(symbol)).toList();
                    if (!positions.isEmpty()) {
                        event.onPositionOpened(positions.getFirst());
                    }
                }
            }
        }

        if (isFeel) {
            Order input = getOrder(item, isClose);
            for (String s : orders.keySet()) {
                if (s.equalsIgnoreCase(symbol)) {
                    OrderEvent event = orders.get(s);
                    event.onOrder(input);
                }
            }
        }
    }

    private Order getOrder(OrderItem item, boolean isClose) {
        Order input = new Order();
        input.setOrderId(item.getOrderId());
        input.setSymbol(symbol);
        input.setPrice(item.getPrice());
        input.setSize(item.getQty());
        input.setOrderType(item.getType());
        input.setStopTraling(false);
        input.setPosSide((item.getSide().equalsIgnoreCase("buy") ? "LONG" : "SHORT"));
        input.setSide(item.getSide());
        input.setClient0Id(item.getOrderId());
        input.setFilledAmount(item.getQty());
        input.setState(item.getOrderStatus());
        input.setOrderSource("API");
        input.setTradeSide(isClose ? "close" : "open");
        input.setMerginCoin("USDT");
        input.setMarginMode("ISOLATION");
        input.setLeverage(item.getLeverage());
        input.setHoldMode(item.getPositionMode());

        return input;
    }

//    private void handleTickerUpdate(PriceResp resp) {
//        try {
//            PriceItem item = resp.getData();
//            BigDecimal current = item.getMarkPrice();
//
//            if (!isStopTrailing.getT()) {
//                if (settings.getStopInProfitVariant(user.getGroup()).equals("xprofit")) {
//                    TradeService ts = user.getUserBeerj();
//                    Position position = ts.getPositions(user).stream().filter(p -> p.getSymbol().equals(symbol)).toList().getFirst();
//                    SymbolInfo info = ts.getSymbolInfo(user, symbol);
//
//                    if (monitor.stopInProfit(user, position, current, stopId, info).succes()) {
//                        isStopTrailing.setT(true);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
}