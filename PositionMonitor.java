package com.plovdev.bot.modules.beerjes.monitoring;

import com.plovdev.bot.modules.beerjes.Order;
import com.plovdev.bot.modules.beerjes.Position;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.models.SymbolInfo;

import java.math.BigDecimal;
import java.util.List;

public interface PositionMonitor {
    OrderResult stopInProfit(UserEntity user, Position position, BigDecimal current, String sid, SymbolInfo info);
    List<OrderResult> cancelLimitOrders(UserEntity user, Order order);
}