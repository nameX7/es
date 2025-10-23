package com.plovdev.bot.modules.beerjes;

import com.plovdev.bot.modules.beerjes.security.EncryptionService;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.exceptions.ApiException;
import com.plovdev.bot.modules.exceptions.InvalidParametresException;
import com.plovdev.bot.modules.exceptions.NetworkException;
import com.plovdev.bot.modules.models.OrderResult;
import com.plovdev.bot.modules.models.PositionsModel;
import com.plovdev.bot.modules.models.SymbolInfo;
import com.plovdev.bot.modules.models.Ticker;
import com.plovdev.bot.modules.parsers.Signal;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Интерфейс для торговли на бирже. Его должны реализовывать классы,
 * которые будут торговать с разными биржами, типа BitGet, или BitUnix.

 * !!!UserEntity нужен!!! без него !!!биржа заблокирет!!! мои запросы. А если что, то потом вырежем его.
 */
public interface TradeService {
    /**
     * Метод, который открывает ордер, и возвращает результат.
     * @param signal сигнал, который нужно открыть. Содержит всю информацию о сигнале
     *               типа targets, stop-loss, tp, type(limit, mraket) и тд.
     * @param user Пользователь, у которого будет открываться сигнал.
     * @return Результат открытия сигнала.
     */
    OrderResult openOrder(Signal signal, UserEntity user, SymbolInfo symbolInfo, BigDecimal entryPrice) throws Exception;
    OrderResult closeOrder(UserEntity user, Order order) throws ApiException;
    OrderResult modifyOrder(UserEntity user, Map<String, String> payload);

    /**
     * Метод для расчета размера позиции в USDT.
     * @param user пользователь.
     * @param entryPrice цена.
     * @param stopLoss стоп-лосс.
     * @return размер позиции в USDT.
     */
    BigDecimal calculatePositionSize(UserEntity user, BigDecimal entryPrice, BigDecimal stopLoss, String side) throws ApiException, InvalidParametresException, NetworkException;

    /**
     * Получает минимальный размер ордера для сделки.
     * @param user пользователь, с ключами от биржи.
     * @param pair пара.
     * @return размер сделки.
     */
    BigDecimal getLotSize(UserEntity user, String pair) throws ApiException, InvalidParametresException, NetworkException;

    /**
     * Возвращает лучшее плечо из ползовательского ввода, и биржевого.
     * @return плечо.
     */
    int getEffectiveLeverage(int maxLever, int userLever);
    void setMarginMode(UserEntity user, String mode, String symbol);

    /**
     * Валидируем АПИ ключи.
     * @return true если ключи валидны, false если нет.
     */
    boolean checkApiKeys(UserEntity user) throws ApiException, NetworkException;

    /**
     * Получает баланс пользователя.
     * @param user юзер у которого проверяе баланс.
     * @return баланс пользователя.
     */
    BigDecimal getBalance(UserEntity user) throws ApiException, NetworkException;

    OrderResult closePosition(UserEntity user, Position position);
    OrderResult placeStopLoss(UserEntity user, Position position, String stopLoss,SymbolInfo info, OrderExecutionContext context);

    /**
     * Возвращает сервис безопасности для конкретной биржи.
     * @return сервис безопасности. нужен для шифрования/дешифрования ключей. public
     */
    EncryptionService getSecurityService();

    List<OrderResult> cancelLimits(UserEntity user, String pair, List<String> ids);

    List<Order> getOrders(UserEntity entity);
    List<Position> getPositions(UserEntity entity);
    List<PositionsModel> getHistoryPositions(UserEntity user);
    OrderResult changeLeverge(UserEntity user, String pair, String side, int leverage);
    BigDecimal getEntryPrice(String pair) throws NetworkException;

    List<Ticker> getAllTickers();
    OrderResult updateStopLoss(UserEntity user, String oId, String symbol, BigDecimal newStop);
    SymbolInfo getSymbolInfo(UserEntity user, String symbol) throws NetworkException;
}