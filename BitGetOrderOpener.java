package com.plovdev.bot.modules.beerjes.bitget;

import com.plovdev.bot.modules.beerjes.BitGetTradeService;
import com.plovdev.bot.modules.beerjes.security.BitGetSecurity;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.parsers.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class BitGetOrderOpener {
    private final Signal signal;
    private final BitGetSecurity security;
    private final BitGetTradeService service;
    private final Logger logger = LoggerFactory.getLogger("OrderOpener");

    public BitGetOrderOpener(Signal s, BitGetSecurity sec, BitGetTradeService ts) {
        signal = s;
        security = sec;
        service = ts;
    }

    public OrderResult changeLeverage(UserEntity user, int lever) {
        System.out.println("Set lever...");
        if (!service.changeLeverge(user, signal.getSymbol(), signal.getDirection().toLowerCase(), lever).succes()) {
            return OrderResult.no();
        } else return OrderResult.ok("Leverage changed", null, null);
    }

    public Map<String, String> placeOrder(UserEntity user, String pair, String side, BigDecimal size, String orderType, BigDecimal price, int leverage) {

        logger.info("Attempting place order: symbol={}, size={}, price={}, direction={}",
                pair, size, price, side);
        // Подготавливаем тело запроса
        Map<String, String> payload = new HashMap<>();
        payload.put("symbol", pair);
        payload.put("productType", "USDT-FUTURES");
        payload.put("marginMode", "isolated");
        payload.put("marginCoin", "USDT");
        payload.put("size", size.toPlainString());
        payload.put("side", side.equalsIgnoreCase("long") ? "buy" : "sell");
        payload.put("tradeSide", "open");
        payload.put("force", "gtc");
        payload.put("orderType", orderType);

        if (price != null) {
            payload.put("price", price.toString());
        }

        return payload;
    }
}