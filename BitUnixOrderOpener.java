package com.plovdev.bot.modules.beerjes.bitunix;

import com.plovdev.bot.modules.beerjes.BitUnixTradeService;
import com.plovdev.bot.modules.beerjes.security.BitUnixSecurity;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.parsers.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class BitUnixOrderOpener {
    private final Signal signal;
    private final BitUnixSecurity security;
    private final BitUnixTradeService service;
    private final Logger logger = LoggerFactory.getLogger("BitUnixOrderOpener");

    public BitUnixOrderOpener(Signal s, BitUnixSecurity sec, BitUnixTradeService ts) {
        signal = s;
        security = sec;
        service = ts;
    }

    public OrderResult changeLeverage(UserEntity user, int lever) {
        System.out.println("Set lever...");
        if (!service.changeLeverge(user, signal.getSymbol(), signal.getDirection().toLowerCase(), lever).succes()) {
            return OrderResult.no();
        } else return OrderResult.ok("Leverage changed", String.valueOf(lever), signal.getSymbol());
    }

    public Map<String, String> placeOrder(String pair, String side, BigDecimal size, String orderType, BigDecimal price, String sl) {

        logger.info("Attempting place order: symbol={}, size={}, price={}, direction={}",
                pair, size, price, side);
        // Подготавливаем тело запроса
        Map<String, String> payload = new HashMap<>();
        payload.put("symbol", pair);
        payload.put("qty", size.toPlainString());
        if (price != null) {
            payload.put("price", price.toString());
        }
        payload.put("side", side.equalsIgnoreCase("long") ? "BUY" : "SELL");
        payload.put("tradeSide", "OPEN");
        payload.put("orderType", orderType);
        payload.put("reduceOnly", "false");
        payload.put("effect", "GTC");

        return payload;
    }
}