package com.plovdev.bot.modules.beerjes.bitget;

import com.plovdev.bot.modules.beerjes.BitGetTradeService;
import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.Position;
import com.plovdev.bot.modules.beerjes.TakeProfitLevel;
import com.plovdev.bot.modules.beerjes.monitoring.BitGetWS;
import com.plovdev.bot.modules.beerjes.utils.BeerjUtils;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class BitGetTakesSetuper {
    private final Logger logger = LoggerFactory.getLogger("TakeSetuper");
    private final com.plovdev.bot.modules.logging.Logger custom = new com.plovdev.bot.modules.logging.Logger();
    private final BitGetTradeService service;
    private final SettingsService settings = new SettingsService();
    private final BitGetStopLossTrailer trailer;
    private final StopInProfitTrigger trigger;

    public BitGetTakesSetuper(StopInProfitTrigger trg, BitGetTradeService service, BitGetStopLossTrailer st) {
        this.service = service;
        trailer = st;
        trigger = trg;
    }


    public List<Map<String, String>> placeTakes(BigDecimal positionSize, List<TakeProfitLevel> tpLevels, String symbol, String direction) {
        System.out.println(positionSize + " - Position size");
        List<Map<String, String>> orders = new ArrayList<>();
        for (TakeProfitLevel level : tpLevels) {
            System.out.println(level);
            BigDecimal size = level.getSize();

            Map<String, String> payload = new HashMap<>();
            payload.put("symbol", symbol); // Добавляем суффикс для фьючерсов!
            payload.put("productType", "USDT-FUTURES"); // В ВЕРХНЕМ РЕГИСТРЕ!
            payload.put("marginMode", "isolated"); // Обязательный параметр
            payload.put("marginCoin", "USDT"); // В ВЕРХНЕМ РЕГИСТРЕ!
            payload.put("size", size.toPlainString());
            payload.put("side", direction.equalsIgnoreCase("long") ? "BUY" : "SELL");
            payload.put("tradeSide", "close");
            payload.put("orderType", "limit");
            payload.put("price", level.getPrice().toPlainString());
            payload.put("force", "gtc"); // Обязательно для лимитных ордеров!
            payload.put("reduceOnly", "yes"); // КРИТИЧЕСКИ ВАЖНО - только закрытие!
            orders.add(payload);
        }
        return orders;
    }

    public void manageTakesInMonitor(BitGetWS ws, String symbol, UserEntity user, List<Map<String, String>> orders, String stopLossId, List<TakeProfitLevel> tpLevels, SymbolInfo info, BigDecimal positionSize, String direction) {
        TypeValueSwitcher<Boolean> isOrdered = new TypeValueSwitcher<>(false);
        List<OrderResult> ids = new ArrayList<>(service.placeOrders(user, symbol, orders).stream().filter(OrderResult::succes).toList());
        System.out.println(ids);

        TakeProfitLevel firstLlevel = tpLevels.getFirst();
        ws.addOrderListener(symbol, inputOrder -> {
            String posSide = inputOrder.getPosSide();
            String tradeSide = inputOrder.getTradeSide();
            String orderId = inputOrder.getOrderId();

            if (tradeSide.equalsIgnoreCase("open")) {
                Position position = service.getPositions(user).stream().filter(p -> p.getSymbol().equals(symbol) && p.getHoldSide().equalsIgnoreCase(direction)).toList().getFirst();
                List<TakeProfitLevel> newTakes = BeerjUtils.reAdjustTakeProfits(position.getTotal(), tpLevels, info, service.getEntryPrice(symbol), direction);
                List<Order> orderList = service.getOrders(user).stream().filter(o -> o.getSymbol().equals(symbol) && o.getTradeSide().equalsIgnoreCase("close")).toList();
                for (int i = 0; i < newTakes.size(); i++) {
                    Order o = orderList.get(i);
                    TakeProfitLevel level = newTakes.get(i);
                    Map<String, String> payload = new HashMap<>();
                    System.out.println(o.getOrderId());
                    payload.put("orderId", o.getOrderId());
                    payload.put("symbol", symbol);
                    payload.put("productType", "USDT-FUTURES");
                    payload.put("newSize", level.getSize().toPlainString());
                    payload.put("newPrice", o.getPrice().toPlainString());
                    payload.put("newClientOid", "BITGET#" + o.getOrderId());

                    ids.set(i, service.modifyOrder(user, payload));
                    tpLevels.set(i, level);
                }
            }

            System.out.println(isOrdered.getT() + " - isOrdered");
            if (!isOrdered.getT()) {
                if (isTakeHit(inputOrder, tpLevels, ids)) {
                    try {
                        logger.info("Fisrt take-profit(id: {}) is hit!", orderId);
                        List<Order> ordersList = service.getOrders(user);
                        List<Order> toCancel = new ArrayList<>();

                        for (Order order : ordersList) {
                            custom.blue("Order to cancel by first tp: symbol: {}, tradeSide: {}", order.getSymbol(), order.getTradeSide());
                            if (order.getSymbol().equals(symbol) && order.getTradeSide().equalsIgnoreCase("open")) {
                                custom.info("To cancel added a new order: {}", order);
                                toCancel.add(order);
                            }
                        }
                        custom.info("Order to cancel by first take: {}", toCancel);
                        for (Order order : toCancel) {
                            service.closeOrder(user, order);
                        }
                        //---------------------------------------LIMITS CANCELED------------------------------------------\\
                        if (settings.getStopInProfitVariant(user.getGroup()).equals("take")) {
                            System.out.println("STOP LOSS before trailing: " + stopLossId);
                            OrderResult stopOrder = trailer.trailStopByFirstTakeHit(user, symbol, posSide, stopLossId, info.getPricePlace());
                            if (stopOrder.succes()) {
                                ws.close();
                            }
                            System.out.println("Stop order: " + stopOrder);
                        }
                        isOrdered.setT(true);
                    } catch (Exception e) {
                        logger.info("Fisrt TP hit error: ", e);
                    }
                }
            }
        });
    }
    private boolean isTakeHit(Order inputOrder, List<TakeProfitLevel> tpLevels, List<OrderResult> ids) {
        logger.info("Data params to calc, is first take hit?");
        tpLevels.sort(Comparator.comparing(TakeProfitLevel::getPrice));
        String posSide = inputOrder.getPosSide();

        logger.info("Order side: {}, Tp levels: {} ids: {}", posSide, tpLevels, ids);
        if (posSide.equalsIgnoreCase("short") || posSide.equalsIgnoreCase("sell")) {
            tpLevels = tpLevels.reversed();
            ids = ids.reversed();
        }
        TakeProfitLevel level = tpLevels.get(trigger.getTakeToTrailNumber());
        logger.info("First level is: {}", level);

        boolean isId = inputOrder.getOrderId().equals(ids.get(trigger.getTakeToTrailNumber()).id());
        boolean isPrice = (inputOrder.getPrice().compareTo(level.getPrice()) == 0);
        boolean isClose = inputOrder.getTradeSide().equalsIgnoreCase("close");

        logger.info("Order id: {}, first id: {}, Is id? - {}", inputOrder.getOrderId(), ids.get(trigger.getTakeToTrailNumber()).id(), isId);

        logger.info("Input order price: {}, first price: {}, Is price? - {}", inputOrder.getPrice(), level.getPrice(), isPrice);
        logger.info("Input order trade side: {}, first trade side: close - must, Is TS? - {}", inputOrder.getTradeSide(), isClose);

        boolean finalResult = isId || (isPrice && isClose);
        logger.info("Final result, is first tp? - {}", finalResult);

        return finalResult;
    }
}