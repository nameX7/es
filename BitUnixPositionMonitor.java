package com.plovdev.bot.modules.beerjes.monitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plovdev.bot.modules.beerjes.BitUnixTradeService;
import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.Position;
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

import static com.plovdev.bot.modules.beerjes.utils.BeerjUtils.calculateNewStopPrice;

public class BitUnixPositionMonitor implements PositionMonitor {
    private static final Logger log = LoggerFactory.getLogger("BitUnix monitor");
    private final BitUnixTradeService service;
    private final SettingsService settings = new SettingsService();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SignalDB signalDB = new SignalDB();

    public BitUnixPositionMonitor(BitUnixTradeService s) {
        service = s;
    }

    @Override
    public OrderResult stopInProfit(UserEntity user, Position position, BigDecimal current, String sid, SymbolInfo info) {
        String symobl = position.getSymbol();
        try {
            String group = user.getGroup();
            String ret = settings.getStopInProfit(group);
            String var = ret.substring(0, ret.indexOf(':'));

            if (var.equalsIgnoreCase("xprofit")) {
                String[] out = ret.substring(ret.indexOf(':') + 1).split(",");
                String f1 = out[0];
                String f2 = out[1];

                log.info("Get entry price");
                checkByProfitPercent(user, position, service.getEntryPrice(symobl), new BigDecimal(f1), new BigDecimal(f2), info.getPricePlace());
            }
            return OrderResult.ok("OK", position.getPosId(), position.getSymbol());
        } catch (Exception e) {
            log.error("Trailing error: ", e);
            return OrderResult.no();
        }
    }

    private void checkByProfitPercent(UserEntity user, Position position, BigDecimal currentPrice, BigDecimal triggerPercent, BigDecimal ofssetPercent, int pP) {
        BigDecimal percent = calculateProfitPercent(position, currentPrice);

        if (percent.compareTo(ofssetPercent) >= 0) {
            BigDecimal newStop = calculateNewStopPrice(position.getHoldSide(), position.getEntryPrice(), ofssetPercent, pP);
            service.updateStopLoss(user, position.getPosId(), position.getSymbol(), newStop);
            System.out.println("stop traling");
        }
    }
    private BigDecimal calculateProfitPercent(Position position, BigDecimal currentPrice) {
        BigDecimal price = position.getMarketPrice();
        if (position.getHoldSide().equalsIgnoreCase("LONG") || position.getHoldSide().equalsIgnoreCase("BUY")) {
            return (currentPrice.add(price)).divide(price, 10, RoundingMode.HALF_UP).multiply(new BigDecimal("100"));
        } else {
            return (price.subtract(currentPrice)).divide(price, 10, RoundingMode.HALF_UP).multiply(new BigDecimal("100"));
        }
    }

    @Override
    public List<OrderResult> cancelLimitOrders(UserEntity user, Order order) {
        log.info("Try cancel limits");
        List<Order> orders = service.getOrders(user).stream().filter(o -> o.getSymbol().equals(order.getSymbol()) && o.getOrderType().equalsIgnoreCase("limit")).toList();
        log.info("Limit orders filtered: {}", orders);
        log.info("Canceling...");
        return service.cancelLimits(user, order.getSymbol(), orders.stream().map(Order::getOrderId).toList());
    }
}