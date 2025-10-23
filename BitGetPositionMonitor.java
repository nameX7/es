package com.plovdev.bot.modules.beerjes.monitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plovdev.bot.modules.beerjes.BitGetTradeService;
import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.Position;
import com.plovdev.bot.modules.beerjes.utils.BeerjUtils;
import com.plovdev.bot.modules.databases.SignalDB;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.models.SettingsService;
import com.plovdev.bot.modules.models.SymbolInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class BitGetPositionMonitor implements PositionMonitor {
    private final Logger log = LoggerFactory.getLogger(BitGetPositionMonitor.class);
    private final BitGetTradeService service;
    private final SettingsService settings = new SettingsService();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SignalDB signalDB = new SignalDB();

    public BitGetPositionMonitor(BitGetTradeService s) {
        service = s;
    }

    @Override
    public OrderResult stopInProfit(UserEntity user, Position position, BigDecimal current, String sid, SymbolInfo info) {
        String group = user.getGroup();
        String ret = settings.getStopInProfit(group);
        String var = ret.substring(0, ret.indexOf(':'));


        if (var.equalsIgnoreCase("xprofit")) {
            String[] out = ret.substring(ret.indexOf(':')+1).split(",");
            String f1 = out[0];
            String f2 = out[1];

            return checkByProfitPercent(user, position.getSymbol(), position.getHoldSide(), position.getEntryPrice(), sid, current, new BigDecimal(f1), new BigDecimal(f2));
        }
        return OrderResult.no();
    }

    private OrderResult checkByProfitPercent(UserEntity user, String symbol, String side, BigDecimal price, String pId, BigDecimal currentPrice, BigDecimal triggerPercent, BigDecimal ofssetPercent) {
        BigDecimal percent = calculateProfitPercent(side, price, currentPrice);
        log.info("Put params: side: {}, price: {}, posId: {}, currentPrice: {}, triggerPercent: {}, offsetPercent: {}", side, price, pId, currentPrice, triggerPercent, ofssetPercent);
        log.info("Percent: {}", percent);
        log.info("Trigger percent: {}", triggerPercent);
        if (percent.compareTo(price) >= 0) {
            log.info("--------------------------------TRALING STOPIN PROFIT----------------------------------");
            log.info("Varint is 'xprofit'");
            log.info("Variant parsed: first param: {}, second param: {}", triggerPercent, ofssetPercent);

            BigDecimal newStop = BeerjUtils.calculateNewStopPrice(side, currentPrice, ofssetPercent, 4);
            return service.updateStopLoss(user, pId, symbol, newStop);
        }
        return OrderResult.no();
    }
    private BigDecimal calculateProfitPercent(String side, BigDecimal price, BigDecimal currentPrice) {
        if (side.equalsIgnoreCase("BUY")) {
            return (currentPrice.subtract(price)).divide(price, 10, RoundingMode.HALF_UP).multiply(new BigDecimal("100"));
        } else {
            return (price.subtract(currentPrice)).divide(price, 10, RoundingMode.HALF_UP).multiply(new BigDecimal("100"));
        }
    }

    @Override
    public List<OrderResult> cancelLimitOrders(UserEntity user, Order order) {
        List<Order> orders = service.getOrders(user).stream().filter(o -> o.getSymbol().equals(order.getSymbol()) && o.getOrderType().equalsIgnoreCase("limit") && o.getTradeSide().equalsIgnoreCase("open")).toList();
        return service.cancelLimits(user, order.getSymbol(), orders.stream().map(Order::getOrderId).toList());
    }
}