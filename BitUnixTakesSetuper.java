package com.plovdev.bot.modules.beerjes.bitunix;

import com.plovdev.bot.modules.beerjes.BitUnixTradeService;
import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.Position;
import com.plovdev.bot.modules.beerjes.TakeProfitLevel;
import com.plovdev.bot.modules.beerjes.monitoring.BitUnixWS;
import com.plovdev.bot.modules.beerjes.utils.BeerjUtils;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class BitUnixTakesSetuper {
    private final Logger logger = LoggerFactory.getLogger("TakeSetuper");
    private final com.plovdev.bot.modules.logging.Logger custom = new com.plovdev.bot.modules.logging.Logger();
    private final BitUnixTradeService service;
    private final SettingsService settings = new SettingsService();
    private final BitUnixStopLossTrailer trailer;
    private final StopInProfitTrigger trigger;

    public BitUnixTakesSetuper(StopInProfitTrigger tgr, BitUnixTradeService service, BitUnixStopLossTrailer st) {
        this.service = service;
        trailer = st;
        trigger = tgr;
    }

    public void manageTakesInMonitor(BitUnixWS ws, String symbol, UserEntity user, List<OrderResult> ids, String stopLossId, List<TakeProfitLevel> tpLevels, SymbolInfo info, String direction) {
        TypeValueSwitcher<Boolean> isOrdered = new TypeValueSwitcher<>(false);

        TakeProfitLevel firstLlevel = tpLevels.getFirst();
        ws.addOrderListener(symbol, inputOrder -> {
            String side = inputOrder.getTradeSide();
            String orderId = inputOrder.getOrderId();

            if (side.equalsIgnoreCase("open")) {

                // Минимальный сон (100-500мс), чтобы дать API закончить транзакцию. 1000мс было слишком долго.
                try {
                    Thread.sleep(1000);

                    // ---------- 1. БЕЗОПАСНЫЙ ПОИСК ПОЗИЦИИ ----------
                    List<Position> foundPositions = service.getPositions(user).stream().filter(p -> {
                        String symb = p.getSymbol();
                        boolean isSymb = symb.equals(symbol);
                        String hs = p.getHoldSide().toLowerCase();
                        boolean isLong = hs.equals("buy") || hs.equals("long");
                        String directionMatch = isLong ? "LONG" : "SHORT";
                        return isSymb && directionMatch.equalsIgnoreCase(direction);
                    }).toList();

                    if (foundPositions.isEmpty()) {
                        logger.warn("Position not found after open event for symbol: {}. Skipping TP adjustment.", symbol);
                        return;
                    }

                    Position position = foundPositions.getFirst();

                    // ---------- 2. БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ РАЗМЕРА ----------
                    BigDecimal totalSize = (position.getTotal() != null) ? position.getTotal() : BigDecimal.ZERO;

                    if (totalSize.compareTo(BigDecimal.ZERO) <= 0) {
                        logger.warn("Position total size is zero or null ({}). Cannot adjust takes.", totalSize);
                        return;
                    }

                    // ---------- 3. ПЕРЕСЧЕТ ОБЪЕМОВ (с гарантией точности из BeerjUtils) ----------
                    // Этот вызов вернет список newTakes, сумма объемов которого ТОЧНО равна totalSize
                    List<TakeProfitLevel> newTakes = BeerjUtils.reAdjustTakeProfitsBU(totalSize, tpLevels, info, service.getEntryPrice(symbol), inputOrder.getPosSide());

                    // ---------- 4. МОДИФИКАЦИЯ ОРДЕРОВ С СОРТИРОВКОЙ ----------
                    // Получаем открытые тейк-профиты
                    List<Order> orderList = service.getOrders(user).stream()
                            .filter(o -> o.getSymbol().equals(symbol) && o.isReduceOnly())
                            // КРИТИЧНО: Сортируем ордера по цене, чтобы гарантировать совпадение с newTakes
                            .sorted(Comparator.comparing(Order::getPrice))
                            .toList();

                    if (orderList.size() != newTakes.size()) {
                        logger.error("CRITICAL MISMATCH: Number of open TP orders ({}) does not match new TP levels ({}). Check logic.",
                                orderList.size(), newTakes.size());
                        return;
                    }

                    // Цикл модификации
                    for (int i = 0; i < orderList.size(); i++) {
                        Order o = orderList.get(i);
                        TakeProfitLevel level = newTakes.get(i);

                        Map<String, String> payload = new HashMap<>();
                        payload.put("orderId", o.getOrderId());
                        payload.put("qty", level.getSize().toPlainString());
                        payload.put("price", o.getPrice().toPlainString());

                        ids.set(i, service.modifyOrder(user, payload));
                    }
                } catch (Exception e) {
                    System.out.println("Sleep error");
                }
            }


            if (orderId.equals(stopLossId)) {
                logger.info("Stop-loss(id: {}) is hit!", orderId);

                List<Order> ordersList = service.getOrders(user);
                List<Order> toCancel = new ArrayList<>();

                for (Order order : ordersList) {
                    if (order.getSymbol().equals(symbol)) {
                        toCancel.add(order);
                    }
                }

                custom.info("Orders to cancel: {}", toCancel);
                service.cancelLimits(user, symbol, toCancel.stream().map(Order::getOrderId).toList());
            }
            System.out.println(isOrdered.getT() + " - isOrdered");
            if (!isOrdered.getT()) {
                if (isTakeHit(inputOrder, tpLevels, ids)) {
                    try {
                        try {
                            logger.info("Fisrt take-profit(id: {}) is hit!", orderId);
                            List<Order> ordersList = service.getOrders(user).stream().filter(order -> {
                                boolean isReduce = order.isReduceOnly();
                                String s = order.getSymbol();
                                logger.info("Is reduce: {}, symbol: {}", isReduce, symbol);
                                return s.equals(symbol) && !isReduce;
                            }).toList();

                            logger.info("Orders to cancel: {}", ordersList);
                            for (Order order : ordersList) {
                                service.closeOrder(user, order);
                            }
                        } catch (Exception e) {
                            logger.error("Err: ", e);
                        }
                        //---------------------------------------LIMITS CANCELED------------------------------------------\\
                        if (trigger.isTakeVariant()) {
                            System.out.println("STOP LOSS before trailing: " + stopLossId);
                            if (trailer.trailStopByFirstTakeHit(user, symbol, side, stopLossId, info.getPricePlace()).succes()) {
                                System.out.println("Stop trailing success");
                            }
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
        if (posSide.equalsIgnoreCase("long") || posSide.equalsIgnoreCase("buy")) {
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