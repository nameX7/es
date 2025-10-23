package com.plovdev.bot.modules.beerjes.bitget;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plovdev.bot.modules.beerjes.Position;
import com.plovdev.bot.modules.beerjes.TradeService;
import com.plovdev.bot.modules.beerjes.utils.BeerjUtils;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.models.SettingsService;
import com.plovdev.bot.modules.models.StopInProfitTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

public class BitGetStopLossTrailer {
    private final Logger log = LoggerFactory.getLogger("StopLossTrailer");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TradeService service;
    private final SettingsService settings;
    private final StopInProfitTrigger trigger;

    public BitGetStopLossTrailer(TradeService ts, StopInProfitTrigger t) {
        service = ts;
        settings = new SettingsService();
        trigger = t;
    }


    public OrderResult trailStopByFirstTakeHit(UserEntity user, String symbol, String side, String oId, int pricePlace) {
        log.info("--------------------------------TRALING STOP IN PROFIT----------------------------------");
        log.info("Stop loss trailing tosymbol: {}", symbol);
        String group = user.getGroup();
        BigDecimal ret = trigger.getStopInProfitPercent();

        log.info("Settings getted: group: {}, ret: {}, profit varint: 'ftake'}", group, ret);

        log.info("Variant is 'ftake'");
        log.info("Fisrt take percent: {}", ret);

        List<Position> positions = service.getPositions(user).stream().filter(p -> p.getSymbol().equals(symbol)).toList();
        return checkByTakeProfit(user, side, oId, positions.getFirst().getEntryPrice(), ret, symbol, pricePlace);
    }

    private OrderResult checkByTakeProfit(UserEntity user, String side, String oId, BigDecimal entry, BigDecimal offsetPercent, String symbol, int pricePlace) {
        try {
            BigDecimal newStop = BeerjUtils.calculateNewStopPrice(side, entry, offsetPercent, pricePlace);
            log.info("Trailing stop activated for order: {}, new stop: {}", oId, newStop);
            return service.updateStopLoss(user, oId, symbol, newStop);
        } catch (Exception e) {
            log.error("Error in checkByTakeProfit for order: {}", oId, e);
            return OrderResult.no();
        }
    }
}