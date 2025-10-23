package com.plovdev.bot.modules.beerjes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.plovdev.bot.modules.beerjes.bitget.BitGetOrderOpener;
import com.plovdev.bot.modules.beerjes.bitget.BitGetStopLossTrailer;
import com.plovdev.bot.modules.beerjes.bitget.BitGetTakesSetuper;
import com.plovdev.bot.modules.beerjes.monitoring.BitGetWS;
import com.plovdev.bot.modules.beerjes.security.BitGetSecurity;
import com.plovdev.bot.modules.beerjes.security.EncryptionService;
import com.plovdev.bot.modules.beerjes.utils.BeerjUtils;
import com.plovdev.bot.modules.databases.UserEntity;
import com.plovdev.bot.modules.exceptions.ApiException;
import com.plovdev.bot.modules.exceptions.InvalidParametresException;
import com.plovdev.bot.modules.exceptions.NetworkException;
import com.plovdev.bot.modules.logging.Colors;
import com.plovdev.bot.modules.models.*;
import com.plovdev.bot.modules.parsers.Signal;
import com.plovdev.bot.modules.parsers.SignalCorrector;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.plovdev.bot.modules.beerjes.utils.BitGetUtils.*;

public class BitGetTradeService implements TradeService {
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectionPool(new ConnectionPool(10, 5, TimeUnit.MINUTES))
            .callTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .build();

    private final Logger logger = LoggerFactory.getLogger("BitGetTradeService");
    private final com.plovdev.bot.modules.logging.Logger custom = new com.plovdev.bot.modules.logging.Logger();
    private final BitGetSecurity security;
    private final SettingsService settings = new SettingsService();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Констуктор класса.
     * Инициализирует сервис, для работы с безопасностью.
     *
     * @param security сервис для работы с безопасностью, шифрованием, и тд.
     */
    public BitGetTradeService(BitGetSecurity security) {
        this.security = security;
    }


    /**
     * Метод, который открывает ордер, и возвращает результат.
     *
     * @param signal сигнал, который нужно открыть. Содержит всю информацию о сигнале
     *               типа targets, stop-loss, tp, type(limit, mraket) и тд.
     * @param user   Пользователь, у которого будет открываться сигнал.
     * @return Результат открытия сигнала.
     */
    @Override
    public OrderResult openOrder(Signal signal, UserEntity user, SymbolInfo symbolInfo, BigDecimal entryPrice) throws Exception {
        printPart("OPEN ORDER");
        SignalCorrector.correct(signal, BeerjUtils.BITGET);
        custom.info(signal.toString());
        System.out.println();

        OrderExecutionContext oec = new OrderExecutionContext(StopInProfitTrigger.load(user.getGroup()));

        String direction = signal.getDirection();
        int effectiveLeverage = getEffectiveLeverage(symbolInfo.getMaxLever(), Integer.parseInt(user.getPlecho()));
        BigDecimal leverage = BigDecimal.valueOf(effectiveLeverage);

        long startTimeOpening = System.currentTimeMillis();
        BitGetOrderOpener opener = new BitGetOrderOpener(signal, security, this);

        String symbol = signal.getSymbol();

        CompletableFuture<Void> marginModeChange = CompletableFuture.supplyAsync(() -> {
            System.out.println("Change margin mode...");
            setMarginMode(user, "isolated", symbol);
            System.out.println("Margin mode changed");
            return null;
        });
        CompletableFuture<OrderResult> changeLeverFuture = CompletableFuture.supplyAsync(() -> changeLeverge(user, symbol, direction.toLowerCase(), effectiveLeverage));
        CompletableFuture<OrderResult> validateOpen = CompletableFuture.supplyAsync(() -> BeerjUtils.valdateOpen(user, signal));
        CompletableFuture<BigDecimal> getPs = CompletableFuture.supplyAsync(() -> BeerjUtils.getPosSize(user, signal, this, entryPrice));

        CompletableFuture.allOf(changeLeverFuture, marginModeChange, validateOpen, getPs).join();

        logger.info("Configurated environment.");
        //----------------------------------------------------------\\
        logger.info("Validating user. Id: {}, name: {}", user.getTgId(), user.getTgName());
        OrderResult canNext = validateOpen.get();
        if (!canNext.succes()) {
            logger.warn("This user cannot next open order. Has open position for symbol: {}", symbol);
            return canNext;
        }
        logger.info("User can open order!\n\n");

        BigDecimal positionSize = getPs.get().multiply(leverage);

        List<String> types = signal.getTypeOreder();
        BigDecimal oneOrderSize = (new BigDecimal("100.0").divide(BigDecimal.valueOf(types.size()), 5, RoundingMode.HALF_EVEN)).divide(new BigDecimal("100.0"), symbolInfo.getPricePlace(), RoundingMode.HALF_EVEN);
        BigDecimal totalSize = setSize(symbolInfo, positionSize.multiply(oneOrderSize).setScale(symbolInfo.getVolumePlace(), RoundingMode.HALF_EVEN));
        custom.warn("Total size before scaling: {}", totalSize);


        logger.info("Getting variable values: positionSize: {}, leverage: {}, direction: {}, types size: {}, types: {}", positionSize, leverage, direction, types.size(), types);
        logger.info("One order size: {}, totalSize: {}\n\n", oneOrderSize, totalSize);

        logger.info("Try change leverage. New lever: {}", effectiveLeverage);

        OrderResult leverResult = changeLeverFuture.get();
        if (!leverResult.succes()) {
            logger.info("Leverage not changed successfuly");
            marginModeChange.get();
            return leverResult;
        }
        logger.info("Leverage changed.\n\n");

        logger.info("Put orders payload.");
        List<Map<String, String>> ordersPayload = new ArrayList<>();
        if (types.contains("market")) {
            ordersPayload.add(opener.placeOrder(user, symbol, direction, totalSize, "market", null, effectiveLeverage));
            logger.info("Added market order to payload.");
        }

        if (types.size() > 1 || !types.contains("market")) {
            BigDecimal totalMargin = BigDecimal.ZERO;
            for (int i = types.contains("market") ? 1 : 0; i < types.size(); i++) {
                ordersPayload.add(opener.placeOrder(user, symbol, direction, totalSize, "limit", new BigDecimal(types.get(i)).setScale(symbolInfo.getPricePlace(), RoundingMode.HALF_EVEN), effectiveLeverage));
                logger.info("Added limit order to payload.");
            }
        }

        logger.info("Batch orders payload formed: {}", ordersPayload);
        List<OrderResult> results = placeOrders(user, symbol, ordersPayload);
        for (OrderResult result : results) {
            if (!result.succes()) {
                return result;
            }
        }

        BitGetWS ws = new BitGetWS(user, security, this);
        if (types.contains("market")) {
            try (ExecutorService placeExecutor = Executors.newSingleThreadExecutor()) {
                CompletableFuture<Void> setupTakes = CompletableFuture.supplyAsync(() -> {
                    try {
                        Position position = getPositions(user).stream().filter(p -> p.getSymbol().equals(symbol) && p.getHoldSide().equalsIgnoreCase(direction)).toList().getFirst();
                        placeStopLoss(user, position, signal.getStopLoss(), symbolInfo, oec);
                        ws.setStopId(oec.getStopLossId());
                        custom.info("Setuped sl: {}", oec.getStopLossId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    oec.setPositioned(true);
                    return null;
                });
                CompletableFuture.allOf(setupTakes).join();

                placeExecutor.execute(() -> {
                    setupTP(signal, user, totalSize, new BigDecimal(signal.getStopLoss()), ws, totalSize, symbolInfo, oec);
                    custom.info("Setuped tp");
                    oec.setPositioned(true);
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        long endTimeOpening = System.currentTimeMillis();
        long totalTimeMillis = endTimeOpening - startTimeOpening;
        long totalTimeSecs = (endTimeOpening - startTimeOpening) / 1000;
        logger.info("TOTALS: Order opened for {}ms({}s)", totalTimeMillis, totalTimeSecs);

        logger.info("Starting position monitor...");
        startPositionMonitor(user, symbol, signal, totalSize, ws, totalSize, symbolInfo, oec);
        logger.info("Position monitor started.");

        return OrderResult.ok("Position placed, userId: " + user.getTgId() + ", tgName: " + user.getTgName(), results.getFirst().id(), symbol);
    }

    private BigDecimal setSize(SymbolInfo symbolInfo, BigDecimal totalSize) {
        BigDecimal sizeMultiplier = symbolInfo.getSizeMultiplier();
        if (!BeerjUtils.isMultiple(totalSize, sizeMultiplier)) {
            totalSize = totalSize.divide(sizeMultiplier, symbolInfo.getVolumePlace(), RoundingMode.HALF_EVEN).multiply(sizeMultiplier);
            if (totalSize.compareTo(symbolInfo.getMinTradeNum()) < 0) {
                totalSize = symbolInfo.getMinTradeNum();
            }
        }
        return totalSize;
    }

    @Override
    public OrderResult closeOrder(UserEntity user, Order order) throws ApiException {
        try {
            long timestamp = System.currentTimeMillis();
            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String phrase = security.decrypt(user.getPhrase());
            String path = "/api/v2/mix/order/cancel-order";
            String method = "POST";

            String id = order.getOrderId();
            String symbol = order.getSymbol();
            System.out.println(id + " - ID");
            System.out.println(symbol + " - SYMBOL");

            Map<String, String> orderMap = new HashMap<>();
            orderMap.put("orderId", id.trim());
            orderMap.put("symbol", symbol);
            orderMap.put("productType", "usdt-futures");
            orderMap.put("marginCoin", "USDT");

            String body = objectMapper.writeValueAsString(orderMap);

            System.err.println(body);
            String sign = security.generateSignature(timestamp + method + path + body, secret);
            // 2. Формируем запрос
            Request request = new Request.Builder()
                    .url(BITGET_API_BASE_URL + path)
                    .post(RequestBody.create(body, MediaType.get("application/json")))
                    .addHeader("ACCESS-KEY", api)
                    .addHeader("ACCESS-SIGN", sign)
                    .addHeader("ACCESS-TIMESTAMP", String.valueOf(timestamp))
                    .addHeader("ACCESS-PASSPHRASE", phrase)
                    .addHeader("Content-Type", "application/json")
                    .build();

            // 3. Отправляем запрос
            custom.info("Отправляем запрос");

            try (Response response = client.newCall(request).execute()) {
                String responseBody = Objects.requireNonNull(response.body()).string();
                custom.warn(responseBody);
                if (validateJsonResopnse(responseBody)) {
                    return OrderResult.ok("ордер отменен", id, symbol);
                } else {
                    return OrderResult.error("no success order canceling", id, symbol);
                }
            }
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки post запроса в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки post запроса: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    @Override
    public OrderResult closePosition(UserEntity user, Position position) throws ApiException {
        try {
            long timestamp = System.currentTimeMillis();
            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String phrase = security.decrypt(user.getPhrase());
            String path = "/api/v2/mix/order/close-positions";
            String method = "POST";

            String pair = position.getSymbol();
            String holdSide = position.getHoldSide();

            Map<String, String> orderMap = new HashMap<>();
            orderMap.put("symbol", pair);
            orderMap.put("productType", "USDT-FUTURES");
            orderMap.put("holdSide", holdSide);

            String body = objectMapper.writeValueAsString(orderMap);

            System.err.println(body);
            String sign = security.generateSignature(timestamp + method + path + body, secret);
            // 2. Формируем запрос
            Request request = new Request.Builder()
                    .url(BITGET_API_BASE_URL + path)
                    .post(RequestBody.create(body, MediaType.get("application/json")))
                    .addHeader("ACCESS-KEY", api)
                    .addHeader("ACCESS-SIGN", sign)
                    .addHeader("ACCESS-TIMESTAMP", String.valueOf(timestamp))
                    .addHeader("ACCESS-PASSPHRASE", phrase)
                    .addHeader("Content-Type", "application/json")
                    .build();

            // 3. Отправляем запрос
            custom.info("Отправляем запрос");

            try (Response response = client.newCall(request).execute()) {
                String responseBody = Objects.requireNonNull(response.body()).string();
                custom.warn(responseBody);
                if (validateJsonResopnse(responseBody)) {
                    return OrderResult.ok("Позиция закрыта", "none", pair);
                } else {
                    return OrderResult.error("no success position closing", "none", pair);
                }
            }
        } catch (IOException e) {
            logger.error("Произошла ошибка закрытия позиции в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка закрытия позиции: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public void printPart(String word, Object... objects) {
        String part = "-".repeat(45);
        word = word.trim().toUpperCase();

        String str = part + word + part;
        custom.blue(str, objects);
    }

    /**
     * Метод для расчета размера позиции в USDT.
     *
     * @param user       пользователь.
     * @param entryPrice цена.
     * @param stopLoss   стоп-лосс.
     * @return размер позиции в USDT.
     */
    @Override
    public BigDecimal calculatePositionSize(UserEntity user, BigDecimal entryPrice, BigDecimal stopLoss, String positionSide) throws NetworkException {
        BigDecimal totalSize;
        custom.log("Params: price: {}, stop-loss: {}", "[INFO]", Colors.Blue.toString(), entryPrice, stopLoss);
        if (entryPrice.compareTo(BigDecimal.ZERO) <= 0 || stopLoss.compareTo(BigDecimal.ZERO) <= 0)
            throw new InvalidParametresException("Цена входа или стоп-лосс выставленны неверно! User ID: " + user.getTgId() + ". Username: " + user.getTgName());

        BigDecimal stopLosssDistantionPercent;
        if (positionSide.equals("LONG")) {
            logger.info("Calcing stop loss distance for long");
            // Для лонга: (entry - stopLoss) / entry
            stopLosssDistantionPercent = (entryPrice.subtract(stopLoss))
                    .divide(entryPrice, 10, RoundingMode.HALF_UP).abs();
        } else {
            // Для шорта: (stopLoss - entry) / entry
            stopLosssDistantionPercent = (stopLoss.subtract(entryPrice))
                    .divide(entryPrice, 10, RoundingMode.HALF_UP).abs();
            logger.info("Calcing stop loss distance for short");
        }
        logger.info("StopLoss distance percent: {}", stopLosssDistantionPercent);

        String varinat = user.getVariant();
        logger.info("Variant: {}", varinat);

        if (varinat.equals("proc")) {
            BigDecimal balance = getBalance(user);
            logger.info("Balance getted: {}", balance);

            BigDecimal percents = new BigDecimal(user.getProc());
            if (percents.compareTo(BigDecimal.ZERO) <= 0) {
                throw new InvalidParametresException("Процент пользователя меньше или равен 0. User ID: \"+ user.getTgId() + \". Username: \" + user.getTgName()-------------- Percent: " + percents);
            }

            BigDecimal riskAmount = BeerjUtils.getPercent(percents, balance);
            System.out.println(entryPrice.doubleValue());
            custom.error(riskAmount.doubleValue() + " - risk");
            BigDecimal size = riskAmount.divide(stopLosssDistantionPercent, 10, RoundingMode.HALF_UP);
            custom.error(size.doubleValue() + " - size");

            custom.log("Пользователь ID: {}, usrname: {}", "[INFO]", Colors.Blue.toString(), user.getTgId(), user.getTgName());
            if (size.compareTo(BigDecimal.TEN) < 0) {
                throw new IllegalArgumentException("size имеет слишком маленькое значение.");
            }

            custom.info("Расчёт размера позиции (риск %):");
            custom.info("Баланс: {} USDT", balance);
            custom.info("Риск: {}% → {} USDT", percents, riskAmount);
            custom.info("Расстояние до стопа: {}%", stopLosssDistantionPercent.multiply(new BigDecimal("100")).toString());
            custom.info("Размер позиции: {} USDT", size);

            totalSize = size;
        } else {
            BigDecimal sum = new BigDecimal(user.getSum());
            custom.info("Расчёт размера позиции (фиксираванная сумма):");
            custom.info("Размер позиции: {} USDT", sum);
            custom.info("цена входа: {}", entryPrice.toPlainString());

            if (sum.compareTo(BigDecimal.TEN) < 0) {
                throw new InvalidParametresException("У пользователя с id: " + user.getTgId() + ", name: " + user.getTgName() + " слишком малая сумма, хм.");
            } else {
                totalSize = sum.subtract(new BigDecimal("0.5"));
                ;
            }
        }
        return totalSize.divide(entryPrice, 10, RoundingMode.HALF_EVEN);
    }

    /**
     * Получает минимальный размер ордера для сделки.
     *
     * @param user пользователь с ключами от биржи. Если его не сделать, то биржа будет блокировать мои запросы, поэтому это критически важно!!!
     * @param pair пара.
     * @return размер сделки.
     */
    @Override
    public BigDecimal getLotSize(UserEntity user, String pair) throws NetworkException {
        JSONArray array = getResponseArray(user, PAIR_LEVERAGE_ENDPOINT_GET + PRODUCT_TYPE + SYMBOL + pair);
        if (array != null) {
            for (int i = 0; i < array.length(); i++) {
                JSONObject tradeNum = array.getJSONObject(i);
                if (tradeNum.has("minTradeNum") && !tradeNum.isNull("minTradeNum")) {
                    return new BigDecimal(tradeNum.getString("minTradeNum"));
                }
            }
        }
        return null;
    }

    /**
     * Возвращает лучшее плечо из ползовательского ввода, и биржевого.
     *
     * @return плечо.
     */
    @Override
    public int getEffectiveLeverage(int max, int user) {
        return Math.min(max, user);
    }

    @Override
    public SymbolInfo getSymbolInfo(UserEntity user, String symbol) {
        SymbolInfo info = new SymbolInfo();
        try {
            JSONArray array = getResponseArray(user, PAIR_LEVERAGE_ENDPOINT_GET + PRODUCT_TYPE + SYMBOL + symbol);
            if (array != null) {
                JSONObject symbolInfo = array.getJSONObject(0);
                info.setSymbol(symbol);
                info.setBaseCoin(symbolInfo.getString("baseCoin"));
                info.setPricePlace(Integer.parseInt(symbolInfo.getString("pricePlace")));
                info.setVolumePlace(Integer.parseInt(symbolInfo.getString("volumePlace")));
                info.setMinTradeNum(new BigDecimal(symbolInfo.getString("minTradeNum")));
                info.setMaxLever(Integer.parseInt(symbolInfo.getString("maxLever")));
                info.setSizeMultiplier(new BigDecimal(symbolInfo.getString("sizeMultiplier")));
            }
        } catch (Exception e) {
            logger.error("Symbol info error: ", e);
        }
        return info;
    }

    private JSONArray getResponseArray(UserEntity user, String end) throws NetworkException {
        printPart("get response array");
        try (Response response = sendToBitGet(security.decrypt(user.getApiKey()), security.decrypt(user.getSecretKey()), security.decrypt(user.getPhrase()), end)) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            System.out.println("RESPONSE BODY" + responseBody);
            boolean success = response.isSuccessful() && validateJsonResopnse(responseBody);
            logger.info("Is succes? {}", success);
            if (success) {
                JSONObject object = new JSONObject(responseBody);
                if (object.has("data") && !object.isNull("data")) {
                    return object.getJSONArray("data");
                }
            } else {
                logger.warn("https error; CODE: {}, BODY: {}", response.code(), responseBody);
            }
        } catch (IOException ex) {
            custom.warn("Ошибка чтения с биржи:  {}", ex.getMessage());
            throw new NetworkException(ex.getMessage());
        } catch (Exception e) {
            logger.error("Ошибка: ", e);
            throw new ApiException(e.getMessage());
        }
        return null;
    }

    private JSONArray getResponseArray(String ary, UserEntity user, String end) throws NetworkException {
        try (Response response = sendToBitGet(security.decrypt(user.getApiKey()), security.decrypt(user.getSecretKey()), security.decrypt(user.getPhrase()), end)) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            System.out.println(responseBody);
            boolean success = response.isSuccessful() && validateJsonResopnse(responseBody);
            if (success) {
                JSONObject object = new JSONObject(responseBody);
                if (object.has("data") && !object.isNull("data")) {
                    JSONObject data = object.getJSONObject("data");
                    if (data.has(ary) && !data.isNull(ary)) {
                        return data.getJSONArray(ary);
                    }
                }
            } else {
                logger.warn("https error; CODE: {}, BODY: {}", response.code(), responseBody);
            }
        } catch (IOException ex) {
            custom.warn("Ошибка чтения с биржи:  {}", ex.getMessage());
            throw new NetworkException(ex.getMessage());
        } catch (Exception e) {
            logger.error("Ошибка: ", e);
            throw new ApiException(e.getMessage());
        }
        return null;
    }

    /**
     * Валидируем АПИ ключи.
     *
     * @param entity пользователь с ключами от биржи. Если его не сделать, то биржа будет блокировать мои запросы, поэтому это критически важно!!!
     * @return true если ключи валидны, false если нет.
     */
    @Override
    public boolean checkApiKeys(UserEntity entity) throws NetworkException {
        custom.info("Keys: ", security.decrypt(entity.getApiKey()), security.decrypt(entity.getSecretKey()), security.decrypt(entity.getPhrase()));
        try (Response response = sendToBitGet(security.decrypt(entity.getApiKey()), security.decrypt(entity.getSecretKey()), security.decrypt(entity.getPhrase()), TEST_ACCOUNT_ENDPOINT_CHECK + PRODUCT_TYPE)) {
            String responseBody = Objects.requireNonNull(Objects.requireNonNull(response).body()).string();

            if (!response.isSuccessful()) {
                logger.error("Ошибка ответа биржи: {} - {}", response.code(), responseBody);
            }

            boolean success = response.isSuccessful() && validateJsonResopnse(responseBody);
            if (success) custom.info("Ключи подтверждены!");
            else custom.warn("Ключи не подтверждены...");

            return success; // Bitget возвращает "00000" при успехе
        } catch (IOException ex) {
            logger.warn("Ошибка чтения ключей биржи: {}", ex.getMessage());
            throw new NetworkException(ex.getMessage());
        }
    }

    public boolean validateJsonResopnse(String json) {
        printPart("validate response");
        logger.info("valiate resp...");
        JSONObject object = new JSONObject(json);
        if (object.has("code") && !object.isNull("code")) {
            return object.getString("code").equals("00000");
        }
        logger.info("returning false...");
        return false;
    }

    /**
     * Получает баланс пользователя.
     *
     * @param user пользователь с ключами от биржи. Если его не сделать, то биржа будет блокировать мои запросы, поэтому это критически важно!!! ктому же у этого пользователя мы и проверяем баланс!
     * @return баланс пользователя.
     */
    @Override
    public BigDecimal getBalance(UserEntity user) throws NetworkException, ApiException {
        JSONArray array = getResponseArray(user, TEST_ACCOUNT_ENDPOINT_CHECK + PRODUCT_TYPE);
        if (array != null) {
            for (int i = 0; i < array.length(); i++) {
                JSONObject available = array.getJSONObject(i);
                if (available.has("available") && !available.isNull("available")) {
                    return new BigDecimal(available.getString("available"));
                }
            }
        }
        return null;
    }

    @Override
    public EncryptionService getSecurityService() {
        return security;
    }

    private Response sendToBitGet(String api, String secret, String phrase, String path) {
        logger.info("Генерируем подпись (HMAC)");
        long timestamp = System.currentTimeMillis();
        String method = "GET";

        String sign = security.generateSignature(timestamp + method + path, secret);
        // 2. Формируем запрос

        logger.info("Формируем запрос");

        Request request = new Request.Builder()
                .url(BITGET_API_BASE_URL + path)
                .addHeader("ACCESS-KEY", api)
                .addHeader("ACCESS-SIGN", sign)
                .addHeader("ACCESS-TIMESTAMP", String.valueOf(timestamp))
                .addHeader("ACCESS-PASSPHRASE", phrase)
                .addHeader("Content-Type", "application/json")
                .build();

        // 3. Отправляем запрос
        custom.info("Отправляем запрос");

        try {
            return client.newCall(request).execute();
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки запроса в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки запроса: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    @Override
    public BigDecimal getEntryPrice(String pair) {
        Request request = new Request.Builder()
                .url(BITGET_API_BASE_URL + "/api/v2/mix/market/symbol-price?productType=usdt-futures&symbol=" + pair)
                .build();

        try (Response response = client.newCall(request).execute()) {
            String body = Objects.requireNonNull(response.body()).string();
            JSONObject object = new JSONObject(body);
            if (object.has("data") && !object.isNull("data")) {
                JSONArray data = object.getJSONArray("data");
                for (int i = 0; i < data.length(); i++) {
                    JSONObject price = data.getJSONObject(i);
                    if (price.has("price") && !price.isNull("price")) return new BigDecimal(price.getString("price"));
                }
            }

        } catch (IOException e) {
            logger.error("Произошла ошибка получения цены: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка полуяения цены: ", e);
        }
        return new BigDecimal("0.0");
    }

    /**
     * Метод, который возвращает сервис безопасности.
     *
     * @return сервис безопаснисти BitGet.
     */
    public BitGetSecurity getSecurity() {
        return security;
    }

    @Override
    public List<OrderResult> cancelLimits(UserEntity user, String pair, List<String> ids) {
        logger.info("Order id to cancel: {}", ids);
        List<OrderResult> results = new ArrayList<>();
        try {
            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String phrase = security.decrypt(user.getPhrase());
            String timestamp = String.valueOf(System.currentTimeMillis());
            String method = "POST";

            String path = "/api/v2/mix/order/batch-cancel-orders";

            List<Map<String, String>> orderList = new ArrayList<>();
            for (String id : ids) {
                Map<String, String> orderMap = new HashMap<>();
                orderMap.put("orderId", id.trim()); // Каждый orderId в отдельном объекте
                orderList.add(orderMap);
            }

            Map<String, Object> omap = new HashMap<>();
            omap.put("symbol", pair);
            omap.put("productType", "usdt-futures");
            omap.put("marginCoin", "USDT");
            omap.put("orderList", orderList);

            String body = objectMapper.writeValueAsString(omap);
            System.err.println(body);
            String sign = security.generateSignature(timestamp + method + path + body, secret);

            // 2. Формируем запрос
            Request request = new Request.Builder()
                    .url(BITGET_API_BASE_URL + path)
                    .post(RequestBody.create(body, MediaType.get("application/json")))
                    .addHeader("ACCESS-KEY", api)
                    .addHeader("ACCESS-SIGN", sign)
                    .addHeader("ACCESS-PASSPHRASE", phrase)
                    .addHeader("ACCESS-TIMESTAMP", timestamp)
                    .addHeader("locale", "en-US")
                    .addHeader("Content-Type", "application/json")
                    .build();

            // 3. Отправляем запрос
            custom.info("Отправляем запрос");

            try (Response response = client.newCall(request).execute()) {
                String responseBody = Objects.requireNonNull(response.body()).string();
                custom.warn(responseBody);
                if (validateJsonResopnse(responseBody)) {
                    //TODO сделать id ордера(достать из ответа)
                    results.add(OrderResult.ok("ордер отменен", null, pair));
                } else {
                    results.add(OrderResult.error("no success orders canceling", "123", pair));
                }
            }
        } catch (IOException e) {
            logger.error("Произошла ошибка отмены ордеров в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отмены ордеров: ", e);
            throw new ApiException(e.getMessage());
        }
        return results;
    }


    @Override
    public List<Order> getOrders(UserEntity user) {
        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());
        String phrase = security.decrypt(user.getPhrase());
        String end = "/api/v2/mix/order/orders-pending?productType=usdt-futures";
        List<Order> orders = new ArrayList<>();

        try {
            JSONArray array = getResponseArray("entrustedList", user, end);
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject object = array.getJSONObject(i);
                    orders.add(new Order(
                            object.getString("symbol").toUpperCase(),
                            new BigDecimal(object.getString("size")),
                            object.getString("orderId"),
                            object.getString("clientOid"),
                            null,
                            object.getString("fee"),
                            new BigDecimal(object.getString("price")),
                            object.getString("status").isEmpty() ? "live" : object.getString("status"),
                            object.getString("side"),
                            null,
                            object.getString("totalProfits"),
                            object.getString("posSide"),
                            object.getString("marginCoin"),
                            null,
                            new BigDecimal(object.getString("presetStopLossPrice").isEmpty() ? "0.0" : object.getString("presetStopLossPrice")),
                            null,
                            object.getString("orderType"),
                            object.getInt("leverage"),
                            object.getString("marginMode"),
                            false,
                            object.getString("tradeSide"),
                            null,
                            object.getString("orderSource"),
                            object.getString("cTime"),
                            object.getString("uTime")
                    ));
                }
            }
        } catch (NetworkException e) {
            logger.error("Error:", e);
        }
        return orders;
    }


    @Override
    public List<Position> getPositions(UserEntity user) {
        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());
        String phrase = security.decrypt(user.getPhrase());
        List<Position> orders = new ArrayList<>();

        try {
            JSONArray array = getResponseArray(user, GET_POSITIONS_ENDPOINT);
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject object = array.getJSONObject(i);
                    orders.add(new Position(
                            null,
                            object.getString("marginCoin"),
                            object.getString("symbol"),
                            object.getString("holdSide"),
                            new BigDecimal(object.getString("openDelegateSize")),
                            new BigDecimal(object.getString("available")),
                            new BigDecimal(object.getString("locked")),
                            new BigDecimal(object.getString("total")),
                            object.getInt("leverage"),
                            new BigDecimal(object.getString("unrealizedPL")),
                            new BigDecimal(object.getString("liquidationPrice")),
                            new BigDecimal(object.getString("keepMarginRate")),
                            new BigDecimal(object.getString("markPrice")),
                            object.getString("cTime"),
                            object.getString("uTime"),
                            new BigDecimal(object.optString("openPriceAvg", "0.0")),
                            false
                    ));
                }
            }
        } catch (NetworkException e) {
            logger.error("Error:", e);
        }
        return orders;
    }

    public void setupTP(Signal signal, UserEntity user, BigDecimal positionSize, BigDecimal stopLoss, BitGetWS ws, BigDecimal margin, SymbolInfo symbolInfo, OrderExecutionContext context) {
        StopInProfitTrigger trigger = context.getTrigger();

        BitGetStopLossTrailer trailer = new BitGetStopLossTrailer(this, trigger);
        BitGetTakesSetuper setuper = new BitGetTakesSetuper(trigger, this, trailer);
        String symbol = signal.getSymbol();
        String direction = signal.getDirection();
        List<TakeProfitLevel> tpLevels;
        try {
            List<BigDecimal> tpRatios = settings.getTPRationsByGroup(user.getGroup()); // например, [30,30,20,10,10]
            List<BigDecimal> takeProfits = signal.getTargets(); // Target 1, 2...
            tpLevels = BeerjUtils.adjustTakeProfits(signal, positionSize, tpRatios, getEntryPrice(symbol), symbolInfo);
            if (tpLevels.size()-1 <= trigger.getTakeToTrailNumber()) {
                trigger.setTakeToTrailNumber(Math.max(tpLevels.size()-2, 0));
            }

            logger.info("Tp level before placing:");
            tpLevels.forEach(l -> logger.info("Level: {}", l));

            System.out.println("Setuping tp... stop loss id: " + context.getStopLossId());
            setuper.manageTakesInMonitor(ws, symbol, user, setuper.placeTakes(positionSize, tpLevels, symbol, direction), context.getStopLossId(), tpLevels, symbolInfo, positionSize, direction);
        } catch (Exception e) {
            logger.error("Critical error in setupTP for user {}: {}", user.getTgId(), e.getMessage(), e);
        }
    }

    public List<OrderResult> placeOrders(UserEntity user, String pair, List<Map<String, String>> orders) {
        List<OrderResult> results = new ArrayList<>();
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("symbol", pair);
            data.put("productType", "usdt-futures");
            data.put("marginMode", "isolated");
            data.put("marginCoin", "USDT");
            data.put("orderList", orders);

            String response = postToBitGet("/api/v2/mix/order/batch-place-order", user, objectMapper.writeValueAsString(data));
            custom.warn("response: {}", response);

            JsonObject responseObject = new Gson().fromJson(response, JsonObject.class);
            JsonObject dataObject = responseObject.getAsJsonObject("data");
            JsonArray successList = dataObject.getAsJsonArray("successList");
            successList.forEach(el -> {
                JsonObject item = el.getAsJsonObject();
                if (item.has("orderId"))
                    results.add(OrderResult.ok("Orders placed!", item.get("orderId").getAsString(), pair));
            });
            JsonArray failureList = dataObject.getAsJsonArray("failureList");
            failureList.forEach(el -> {
                JsonObject item = el.getAsJsonObject();
                if (item.has("errorMsg") && item.has("orderId"))
                    results.add(OrderResult.error(item.get("errorMsg").getAsString(), item.get("orderId").getAsString(), pair));
            });
            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Orders not setupeds");
            } else {
                logger.info("Orders placed successfully: {}", root.path("data").path("orderId").asText());

            }


        } catch (Exception e) {
            logger.error("Orders failed for symbol {}: {}", pair, e.getMessage());
            throw new ApiException("Orders placement failed: " + e.getMessage());
        }
        return results;
    }

    @Override
    public OrderResult placeStopLoss(UserEntity user, Position position, String stopLoss, SymbolInfo info, OrderExecutionContext context) {
        BigDecimal sl = new BigDecimal(stopLoss).setScale(info.getPricePlace(), RoundingMode.HALF_EVEN);
        stopLoss = sl.toPlainString();
        String direction = position.getHoldSide();
        String symbol = position.getSymbol();

        custom.blue("-----------------------------------PLACE STOP-LOSS----------------------------------");
        custom.info("StopLoss: {}, holdSide: {}, symbol: {}", stopLoss, direction, symbol);

        try {
            Map<String, Object> data = new HashMap<>();
            data.put("symbol", symbol);
            data.put("productType", "usdt-futures");
            data.put("marginMode", "isolated");
            data.put("marginCoin", "USDT");
            data.put("planType", "pos_loss");
            data.put("triggerPrice", stopLoss);
            data.put("triggerType", "fill_price");
            data.put("executePrice", "0");
            data.put("holdSide", direction);

            String payload = objectMapper.writeValueAsString(data);
            logger.info("Payload formed: {}", payload);

            String response = postToBitGet("/api/v2/mix/order/place-tpsl-order", user, payload);
            custom.warn("RESPONSE: {}", response);

            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Stop loss not placed");
            } else {
                String oId = root.path("data").path("orderId").asText();
                context.setStopLossId(oId);
                logger.info("Stop loss placed success: {}", oId);

                return OrderResult.ok("Stoploss placed", oId, symbol);
            }

        } catch (Exception e) {
            logger.error("Stop loss failed for symbol {}: {}", symbol, e.getMessage());
            return OrderResult.error(e.getMessage(), "none", symbol);
        }
        custom.blue("------------------------------------STOP-LOSS PLACED----------------------------------");
        return OrderResult.no();
    }


    public String postToBitGet(String endpoint, UserEntity user, String params) {
        logger.info("Генерируем POST подпись (HMAC)");
        System.err.println("Endpoint: --> " + endpoint);
        String timestamp = String.valueOf(System.currentTimeMillis());
        String method = "POST";

        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());
        String phrase = security.decrypt(user.getPhrase());

        String sign = security.generateSignature(timestamp + method + endpoint + params, secret);
        // 2. Формируем запрос

        logger.info("Формируем POST запрос");
        Request request = new Request.Builder()
                .url(BITGET_API_BASE_URL + endpoint)
                .addHeader("ACCESS-KEY", api)
                .addHeader("ACCESS-SIGN", sign)
                .addHeader("ACCESS-TIMESTAMP", timestamp)
                .addHeader("ACCESS-PASSPHRASE", phrase)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(params, MediaType.get("application/json")))
                .build();

        // 3. Отправляем запрос
        custom.info("Отправляем запрос");

        try (Response response = client.newCall(request).execute()) {
            return Objects.requireNonNull(response.body()).string();
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки POST запроса в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки запроса: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public OrderResult updateStopLoss(UserEntity user, String orderId, String symbol, BigDecimal newStop) {
        System.out.println("Trailing stop loss...");

        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", orderId);
        payload.put("marginCoin", "USDT");
        payload.put("productType", "USDT-FUTURES");
        payload.put("symbol", symbol);
        payload.put("triggerPrice", newStop.toPlainString());
        payload.put("triggerType", "fill_price");
        payload.put("executePrice", "0");
        payload.put("size", "");


        try {

            String response = postToBitGet("/api/v2/mix/order/modify-tpsl-order", user, objectMapper.writeValueAsString(payload));
            logger.info("\n\n\nBitGet stop traling response for user: {}, body: {}\n\n\n", user.getTgName(), response);
            JsonNode root = objectMapper.readTree(response);

            if (!validateJsonResopnse(response)) {
                return OrderResult.error("Stop loss trailing fail: " + root.path("msg"), orderId, symbol);
            }
        } catch (Exception e) {
            return OrderResult.error(e.getMessage(), orderId, symbol);
        }
        System.out.println("Stop loss trailing success");
        return OrderResult.ok("OK", orderId, symbol);
    }

    @Override
    public List<PositionsModel> getHistoryPositions(UserEntity user) {
        List<PositionsModel> orders = new ArrayList<>();

        try {
            JSONArray array = getResponseArray("list", user, GET_HISTORY_POSITIONS_ENDPOINT);
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject object = array.getJSONObject(i);
                    orders.add(new PositionsModel(
                            object.getString("positionId"),
                            object.getString("symbol"),
                            object.getString("holdSide"),
                            null,
                            user.getPlecho(),
                            object.getString("pnl"),
                            object.getString("openAvgPrice"),
                            object.getString("closeAvgPrice"),
                            "close",
                            object.getString("netProfit")
                    ));
                }
            }
        } catch (NetworkException e) {
            logger.error("Error:", e);
        }
        return orders;
    }

    @Override
    public OrderResult changeLeverge(UserEntity user, String pair, String side, int leverage) {
        try {
            long timestamp = System.currentTimeMillis();
            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String phrase = security.decrypt(user.getPhrase());
            String path = "/api/v2/mix/account/set-leverage";
            String method = "POST";


            Map<String, String> orderMap = new HashMap<>();
            orderMap.put("symbol", pair);
            orderMap.put("productType", "usdt-futures");
            orderMap.put("marginCoin", "USDT");
            orderMap.put("leverage", String.valueOf(leverage));
            orderMap.put("holdSide", side);

            String body = objectMapper.writeValueAsString(orderMap);
            System.err.println(body);

            String sign = security.generateSignature(timestamp + method + path + body, secret);
            // 2. Формируем запрос
            Request request = new Request.Builder()
                    .url(BITGET_API_BASE_URL + path)
                    .post(RequestBody.create(body, MediaType.get("application/json")))
                    .addHeader("ACCESS-KEY", api)
                    .addHeader("ACCESS-SIGN", sign)
                    .addHeader("ACCESS-TIMESTAMP", String.valueOf(timestamp))
                    .addHeader("ACCESS-PASSPHRASE", phrase)
                    .addHeader("Content-Type", "application/json")
                    .build();

            // 3. Отправляем запрос
            custom.info("Отправляем запрос");

            try (Response response = client.newCall(request).execute()) {
                String responseBody = Objects.requireNonNull(response.body()).string();
                custom.warn(responseBody);
                if (validateJsonResopnse(responseBody)) {
                    return OrderResult.ok("Плечо сменено", "id", pair);
                } else {
                    return OrderResult.error("Invalid ti", "id", pair);
                }
            }
        } catch (IOException e) {
            logger.error("Произошла ошибка смены плеча в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка смены плеча в BitGet: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public void startPositionMonitor(UserEntity user, String symbol, Signal signal, BigDecimal size, BitGetWS ws, BigDecimal margin, SymbolInfo info, OrderExecutionContext context) {
        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());
        String phrase = security.decrypt(user.getPhrase());

        Thread.startVirtualThread(() -> {
            try {
                ws.connect();
                ws.waitForConnection();
                ws.waitForAuthentication();

                ws.addSignal(symbol);

                ws.addPositionListener(symbol, position -> {
                    custom.info("Position monitor worked.");
                    if (!context.isPositioned()) {
                        placeStopLoss(user, position, signal.getStopLoss(), info, context);
                        ws.setStopId(context.getStopLossId());
                        custom.info("Setuped sl: {}", context.getStopLossId());

                        setupTP(signal, user, size, new BigDecimal(signal.getStopLoss()), ws, margin, info, context);
                        custom.info("Setuped tp");

                        context.setPositioned(true);
                    }
                });
            } catch (Exception e) {
                logger.error("WebSocket ERROR: ", e);
            }
        });
    }

    @Override
    public List<Ticker> getAllTickers() {
        List<Ticker> tickers = new ArrayList<>();
        Request request = new Request.Builder()
                .url(BITGET_API_BASE_URL + GET_SYMBOLS)
                .build();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = Objects.requireNonNull(response.body()).string();

            JSONObject ret = new JSONObject(responseBody);
            JSONArray data = ret.getJSONArray("data");

            for (int i = 0; i < data.length(); i++) {
                JSONObject ticker = data.getJSONObject(i);
                tickers.add(new Ticker(ticker.getString("symbol"), new BigDecimal(ticker.optString("lastPr", "0.0")), new BigDecimal(ticker.optString("markPrice", "0.0"))));
            }
        } catch (Exception e) {
            logger.error("ERR ", e);
        }

        return tickers;
    }

    @Override
    public OrderResult modifyOrder(UserEntity user, Map<String, String> data) {
        custom.blue("-----------------------------------MODIFY ORDER----------------------------------");
        custom.info("orderId: {}, payload: {}", data.get("orderId"), data);

        try {
            String payload = objectMapper.writeValueAsString(data);
            logger.info("Payload to modify formed: {}", payload);

            String response = postToBitGet("/api/v2/mix/order/modify-order", user, payload);
            custom.warn("RESPONSE: {}", response);

            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Order not modified");
            } else {
                String oId = root.path("data").path("orderId").asText();
                logger.info("Order modify success: {}", oId);

                return OrderResult.ok("Order modify success", oId, data.get("symbol"));
            }

        } catch (Exception e) {
            String symbol = data.get("symbol");
            logger.error("Modify order failed for symbol {}: {}", symbol, e.getMessage());
            return OrderResult.error(e.getMessage(), "none", symbol);
        }
        custom.blue("------------------------------------ORDER MODIFIED----------------------------------");
        return OrderResult.no();
    }

    @Override
    public void setMarginMode(UserEntity user, String mode, String symbol) {
        Map<String, String> data = new HashMap<>();
        data.put("symbol", symbol);
        data.put("productType", "USDT-FUTURES");
        data.put("marginCoin", "USDT");
        data.put("marginMode", mode);
        try {
            String payload = objectMapper.writeValueAsString(data);
            logger.info("Payload to set margin mode: {}", payload);

            String response = postToBitGet("/api/v2/mix/account/set-margin-mode", user, payload);
            custom.warn("RESPONSE: {}", response);

            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Margin mode not changed(");
            } else {
                String oId = root.path("data").path("orderId").asText();
                logger.info("Margin mode changed success: {}", oId);
            }

        } catch (Exception e) {
            logger.error("Margin mode changed success {}: {}", symbol, e.getMessage());
        }
    }
}