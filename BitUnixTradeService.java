package com.plovdev.bot.modules.beerjes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.plovdev.bot.modules.beerjes.bitunix.BitUnixOrderOpener;
import com.plovdev.bot.modules.beerjes.bitunix.BitUnixStopLossTrailer;
import com.plovdev.bot.modules.beerjes.bitunix.BitUnixTakesSetuper;
import com.plovdev.bot.modules.beerjes.monitoring.BitUnixWS;
import com.plovdev.bot.modules.beerjes.security.BitGetSecurity;
import com.plovdev.bot.modules.beerjes.security.BitUnixSecurity;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.plovdev.bot.modules.beerjes.utils.BitUnixUtils.*;

public class BitUnixTradeService implements TradeService {
    private final Logger logger = LoggerFactory.getLogger("BitUnixTradeService");
    private final com.plovdev.bot.modules.logging.Logger custom = new com.plovdev.bot.modules.logging.Logger();
    private final BitUnixSecurity security;
    private final SettingsService settingsService = new SettingsService();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Констуктор класса.
     * Инициализирует сервис, для работы с безопасностью.
     *
     * @param security сервис для работы с безопасностью, шифрованием, и тд.
     */
    public BitUnixTradeService(BitUnixSecurity security) {
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
    public OrderResult openOrder(Signal signal, UserEntity user, SymbolInfo symbolInfo, BigDecimal entry) throws Exception {
        printPart("place order");
        long startOpenTime = System.currentTimeMillis();
        SignalCorrector.correct(signal, BeerjUtils.BITUNIX);
        custom.info(signal.toString());
        System.out.println();
        System.out.println();

        OrderExecutionContext oec = new OrderExecutionContext(StopInProfitTrigger.load(user.getGroup()));

        String symbol = signal.getSymbol();
        String direction = signal.getDirection();
        String strategy = user.getGroup().toLowerCase();
        String srcFrom = signal.getSrc().toLowerCase();
        BitUnixOrderOpener opener = new BitUnixOrderOpener(signal, security, this);

        int effectiveLeverage = getEffectiveLeverage(symbolInfo.getMaxLever(), Integer.parseInt(user.getPlecho()));
        BigDecimal leverage = BigDecimal.valueOf(effectiveLeverage);

        CompletableFuture<OrderResult> changeLeverFuture = CompletableFuture.supplyAsync(() -> changeLeverge(user, symbol, direction, effectiveLeverage));
        CompletableFuture<Void> changeMarginModeFuture = CompletableFuture.supplyAsync(() -> {
            setMarginMode(user, "ISOLATION", symbol);
            return null;
        });
        CompletableFuture<OrderResult> validateOpen = CompletableFuture.supplyAsync(() -> BeerjUtils.valdateOpen(user, signal));
        CompletableFuture<BigDecimal> getPs = CompletableFuture.supplyAsync(() -> BeerjUtils.getPosSize(user, signal, this, entry));
        CompletableFuture.allOf(changeLeverFuture, changeMarginModeFuture, validateOpen, getPs).join();

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
        BigDecimal oneOrderSize = (new BigDecimal("100.0").divide(BigDecimal.valueOf(types.size()), symbolInfo.getPricePlace(), RoundingMode.HALF_EVEN)).divide(new BigDecimal("100.0"), 2, RoundingMode.HALF_EVEN);
        BigDecimal totalSize = setSize(symbolInfo, positionSize.multiply(oneOrderSize).setScale(symbolInfo.getVolumePlace(), RoundingMode.HALF_EVEN));
        custom.warn("Total size before scaling: {}", totalSize);

        logger.info("Try change leverage. New lever: {}", effectiveLeverage);
        OrderResult leverResult = changeLeverFuture.get();
        if (!leverResult.succes()) {
            logger.info("Leverage not changed successfuly");
            return leverResult;
        }
        changeMarginModeFuture.get();
        logger.info("Leverage changed.\n\n");


        logger.info("Getting variable values: positionSize: {}, leverage: {}, direction: {}, types size: {}, types: {}", positionSize, leverage, direction, types.size(), types);
        logger.info("One order size: {}, totalSize: {}\n\n", oneOrderSize, totalSize);

        logger.info("Put orders payload.");
        List<Map<String, String>> ordersPayload = new ArrayList<>();
        if (types.contains("market")) {
            ordersPayload.add(opener.placeOrder(symbol, direction, totalSize, "MARKET", null, signal.getStopLoss()));
            logger.info("Added market order to payload.");
        }

        if (types.size() > 1 || !types.contains("market")) {
            BigDecimal totalMargin = BigDecimal.ZERO;
            for (int i = types.contains("market") ? 1 : 0; i < types.size(); i++) {
                ordersPayload.add(opener.placeOrder(symbol, direction, totalSize, "LIMIT", new BigDecimal(types.get(i)), signal.getStopLoss()));
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

        BitUnixWS ws = new BitUnixWS(user, this, symbol);
        if (types.contains("market")) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            try {
                CompletableFuture<Position> positionFuture = CompletableFuture.supplyAsync(() -> getPositions(user).stream().filter(p -> {
                    System.out.println("Pos symbol: " + p.getSymbol() + ", our symbol: " + symbol);
                    return p.getSymbol().equalsIgnoreCase(symbol);
                }).toList().getFirst());
                Position position = positionFuture.get();

                CompletableFuture<Void> setupStop = CompletableFuture.supplyAsync(() -> {
                    try {
                        OrderResult stopLossResult = placeStopLoss(user, position, signal.getStopLoss(), symbolInfo, oec);
                        custom.info("Setuped sl: {}", stopLossResult.id());
                        ws.setStopId(position.getPosId());
                    } catch (Exception e) {
                        logger.error("Error: ", e);
                    }
                    oec.setPositioned(true);
                    return null;
                });
                CompletableFuture<Void> setupTakes = CompletableFuture.supplyAsync(() -> {
                    setupTP(signal, user, totalSize, new BigDecimal(signal.getStopLoss()), position.getPosId(), totalSize, symbolInfo, ws, oec);
                    custom.info("Setuped tp");
                    oec.setPositioned(true);
                    return null;
                });

                CompletableFuture.allOf(setupStop, setupTakes).join();
            } catch (Exception e) {
                logger.error("Error: ", e);
            }
        }

        logger.info("Starting position monitor...");
        startPositionMonitor(user, symbol, signal, totalSize, ws, totalSize, symbolInfo, oec);
        logger.info("Position monitor started.");

        long endTimeOpening = System.currentTimeMillis();
        long totalTimeMillis = endTimeOpening - startOpenTime;
        long totalTimeSecs = (endTimeOpening - startOpenTime) / 1000;
        logger.info("TOTALS: Order opened for {}ms({}s)", totalTimeMillis, totalTimeSecs);

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

    public OrderResult updateStopLoss(UserEntity user, String positionId, String pair, BigDecimal newStop) {
        printPart("updating stop-loss");
        System.out.println("\n\n");

        try {
            Map<String, Object> data = new HashMap<>();
            data.put("symbol", pair);
            data.put("positionId", positionId);
            data.put("slPrice", newStop.toPlainString());

            String payload = objectMapper.writeValueAsString(data);
            custom.warn(payload);

            String response = postToBitUnix("/api/v1/futures/tpsl/position/modify_order", user, payload);
            logger.info("\n\n\nBitunix stop traling response for user: {}, body: {}\n\n\n", user.getTgName(), response);
            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Stop loss not trailing");
            } else {
                String oId = root.path("data").path("orderId").asText();
                logger.info("Stop loss trailing success: {}", oId);

                return OrderResult.ok("Stop loss trailed", oId, pair);
            }

        } catch (Exception e) {
            logger.error("Stop loss trailing failed for symbol {}: {}", pair, e.getMessage());
            return OrderResult.error(e.getMessage(), "none", pair);
        }
        return OrderResult.no();
    }

    public void printPart(String word, Object... objects) {
        String part = "-".repeat(45);
        word = word.trim().toUpperCase();

        String str = part + word + part;
        custom.blue(str, objects);
    }

    @Override
    public OrderResult closeOrder(UserEntity user, Order order) {
        return cancelLimits(user, order.getSymbol(), List.of(order.getOrderId())).getFirst();
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
    public BigDecimal calculatePositionSize(UserEntity user, BigDecimal entryPrice, BigDecimal stopLoss, String positionSide) {
        BigDecimal totalSize;
        if (entryPrice.compareTo(BigDecimal.ZERO) <= 0 || stopLoss.compareTo(BigDecimal.ZERO) <= 0)
            throw new InvalidParametresException("Цена входа или стоп-лосс выставленны неверно! User ID: " + user.getTgId() + ". Username: " + user.getTgName());


        BigDecimal stopLosssDistantionPercent;
        if (positionSide.equals("LONG")) {
            // Для лонга: (entry - stopLoss) / entry
            stopLosssDistantionPercent = (entryPrice.subtract(stopLoss))
                    .divide(entryPrice, 10, RoundingMode.HALF_UP).abs();
        } else {
            // Для шорта: (stopLoss - entry) / entry
            stopLosssDistantionPercent = (stopLoss.subtract(entryPrice))
                    .divide(entryPrice, 10, RoundingMode.HALF_UP).abs();
        }

        String varinat = user.getVariant();

        if (varinat.equals("proc")) {
            BigDecimal balance = getBalance(user);
            BigDecimal percents = new BigDecimal(user.getProc());
            if (percents.compareTo(BigDecimal.ZERO) <= 0) {
                throw new InvalidParametresException("Процент пользователя меньше или равен 0. User ID: \"+ user.getTgId() + \". Username: \" + user.getTgName()-------------- Percent: " + percents);
            }

            BigDecimal riskAmount = BeerjUtils.getPercent(balance, percents);
            BigDecimal size = riskAmount.divide(stopLosssDistantionPercent, 10, RoundingMode.HALF_UP);

            custom.log("Пользователь ID: {}, usrname: {}", "[INFO]", Colors.Blue.toString(), user.getTgId(), user.getTgName());
            if (size.compareTo(BigDecimal.TEN) < 0)
                throw new IllegalArgumentException("size имеет слишком маленькое значение: " + size.toPlainString());

            custom.info("Расчёт размера позиции (риск %):");
            custom.info("Баланс: {} USDT", balance);
            custom.info("Размер позиции: {} USDT", size);
            custom.info("цена входа: {}", entryPrice.toPlainString());


            totalSize = size;
        } else {
            BigDecimal sum = new BigDecimal(user.getSum());
            custom.info("Расчёт размера позиции (фиксираванная сумма):");
            custom.info("Размер позиции: {} USDT", sum);
            custom.info("цена входа: {}", entryPrice.toPlainString());

            if (sum.compareTo(BigDecimal.TEN) < 0) {
                throw new InvalidParametresException("У пользователя с id: " + user.getTgId() + ", name: " + user.getTgName() + " слишком малая сумма, хм.");
            } else {
                totalSize = sum.subtract(new BigDecimal("0.5"));;
            }
        }
        totalSize = totalSize.divide(entryPrice, 10, RoundingMode.HALF_EVEN);
        custom.info("И того в base coin: {}", totalSize.toPlainString());
        return totalSize;
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
        JSONArray array = getResponseArray("https://fapi.bitunix.com/api/v1/futures/market/trading_pairs?symbols=" + pair);
        if (array != null) {
            for (int i = 0; i < array.length(); i++) {
                JSONObject tradeNum = array.getJSONObject(i);
                if (tradeNum.has("minTradeVolume") && !tradeNum.isNull("minTradeVolume")) {
                    return new BigDecimal(tradeNum.getString("minTradeVolume"));
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

    private JSONArray getResponseArray(String end) throws NetworkException {
        try (Response response = sendToBitUnix(end)) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            boolean success = response.isSuccessful() && validateJsonResopnse(responseBody);
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

    private JSONArray getAuthResponseArray(String end, String api, String secret, String ary) throws NetworkException {
        try (Response response = sendToBitUnixWithAuth(end, api, secret)) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            custom.info(responseBody);
            boolean success = response.isSuccessful() && validateJsonResopnse(responseBody);
            if (success) {
                JSONObject object = new JSONObject(responseBody);
                if (object.has(ary) && !object.isNull(ary)) {
                    return object.getJSONArray(ary);
                }
            } else {
                logger.warn("Error; CODE: {}, BODY: {}", response.code(), responseBody);
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

    private JSONArray getAuthResponseArray(String array, String end, String api, String secret, String ary) throws NetworkException {
        try (Response response = sendToBitUnixWithAuth(end, api, secret)) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            custom.info(responseBody + " - response body");
            boolean success = response.isSuccessful() && validateJsonResopnse(responseBody);
            if (success) {
                JSONObject object = new JSONObject(responseBody);
                if (object.has(array) && !object.isNull(array)) {
                    JSONObject ar = object.getJSONObject(array);
                    if (ar.has(ary) && !ar.isNull(ary)) {
                        return ar.getJSONArray(ary);
                    }
                }
            } else {
                logger.warn("Error; CODE: {}, BODY: {}", response.code(), responseBody);
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
        System.err.println("BitUnixCheck");
        try {
            String responseBody = getFutureAccountMargin(entity);
            System.out.println(responseBody);

            boolean success = validateJsonResopnse(responseBody);
            if (success) custom.info("Ключи подтверждены!");
            else custom.warn("Ключи не подтверждены...");

            return success;
        } catch (Exception ex) {
            logger.warn("Ошибка чтения ключей биржи: {}", ex.getMessage());
            throw new NetworkException(ex.getMessage());
        }
    }

    public boolean validateJsonResopnse(String json) {
        JSONObject object = new JSONObject(json);
        boolean code = false;
        boolean msg = false;

        if (object.has("code") && !object.isNull("code")) {
            code = object.getInt("code") == 0;
        }
        if (object.has("msg") && !object.isNull("msg")) {
            msg = object.getString("msg").toLowerCase().contains("success");
        }
        return code && msg;
    }

    /**
     * Получает баланс пользователя.
     *
     * @param user пользователь с ключами от биржи. Если его не сделать, то биржа будет блокировать мои запросы, поэтому это критически важно!!! ктому же у этого пользователя мы и проверяем баланс!
     * @return баланс пользователя.
     */
    @Override
    public BigDecimal getBalance(UserEntity user) throws ApiException {
        try {
            String response = getFutureAccountMargin(user);
            System.out.println(response);
            JSONObject object = new JSONObject(response);
            if (object.has("data") && !object.isNull("data")) {
                JSONObject data = object.getJSONObject("data");
                if (data.has("available") && !data.isNull("available")) {
                    return new BigDecimal(data.getString("available"));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return new BigDecimal("0.0");
    }

    @Override
    public OrderResult closePosition(UserEntity user, Position position) {
        try {
            long timestamp = System.currentTimeMillis();
            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String path = "/api/v1/futures/trade/flash_close_position";
            String nonce = generateNonce();
            String method = "POST";

            Map<String, String> orderMap = new HashMap<>();
            orderMap.put("positionId", position.getPosId());

            String body = objectMapper.writeValueAsString(orderMap);

            System.err.println(body);
            String sign = generateSignApi(api, secret, nonce, String.valueOf(timestamp), method, body);
            // 2. Формируем запрос
            OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                    .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
            Request request = new Request.Builder()
                    .url(BITUNIX_API_BASE_URL + path)
                    .post(RequestBody.create(body, MediaType.get("application/json")))
                    .addHeader("api-key", api)
                    .addHeader("sign", sign)
                    .addHeader("nonce", nonce)
                    .addHeader("timestamp", String.valueOf(timestamp))
                    .addHeader("language", "en-US")
                    .addHeader("Content-Type", "application/json")
                    .build();

            // 3. Отправляем запрос
            custom.info("Отправляем запрос");

            try (Response response = client.newCall(request).execute()) {
                String responseBody = Objects.requireNonNull(response.body()).string();
                custom.warn(responseBody);
                if (validateJsonResopnse(responseBody)) {
                    return OrderResult.ok("Позиция закрыта", "none", position.getSymbol());
                } else {
                    return OrderResult.error("no success position closing", position.getPosId(), position.getSymbol());
                }
            }
        } catch (IOException e) {
            logger.error("Произошла ошибка закрытия позиции в BitUnix: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка закрытия позиции: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    @Override
    public OrderResult placeStopLoss(UserEntity user, Position position, String stopLoss, SymbolInfo info, OrderExecutionContext context) {
        custom.blue("---------------------------------PLACE STOP-LOSS-------------------------");
        custom.info("StopLoss: {}, holdSide: {}, symbol: {}", stopLoss, position.getHoldSide(), position.getSymbol());

        String pair = position.getSymbol();

        try {
            Map<String, Object> data = new HashMap<>();
            data.put("symbol", pair);
            data.put("positionId", position.getPosId());
            data.put("slPrice", stopLoss);

            String payload = objectMapper.writeValueAsString(data);
            logger.info("Payload formed: {}", payload);

            String response = postToBitUnix("/api/v1/futures/tpsl/position/place_order", user, payload);
            custom.warn(response);
            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Stop loss not placed");
            } else {
                String oId = root.path("data").path("orderId").asText();
                context.setStopLossId(position.getPosId());
                logger.info("Stop loss placed success: {}", oId);

                return OrderResult.ok("Stoploss placed", oId, pair);
            }

        } catch (Exception e) {
            logger.error("Stop loss failed for symbol {}: {}", pair, e.getMessage());
            return OrderResult.error(e.getMessage(), "none", pair);
        }
        custom.blue("---------------------------PLACED STOP-LOSS--------------------------------");
        return OrderResult.no();
    }

    @Override
    public EncryptionService getSecurityService() {
        return security;
    }

    private Response sendToBitUnix(String path) {
        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
        Request request = new Request.Builder()
                .url(path)
                .build();

        // 3. Отправляем запрос


        try {
            return client.newCall(request).execute();
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки запроса в BitUnix: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки запроса: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    private Response sendToBitUnixWithAuth(String path, String api, String sec) throws Exception {
        String nonce = generateNonce();
        String timestamp = String.valueOf(System.currentTimeMillis());
        String method = "GET";

        // Данные с параметром marginCoin
        Map<String, String> data = new HashMap<>();

        String sign = generateSignApi(api, sec, nonce, timestamp, method, data);

        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
        Request request = new Request.Builder()
                .url(BITUNIX_API_BASE_URL + path)
                .header("api-key", api)
                .header("sign", sign)
                .header("nonce", nonce)
                .header("timestamp", timestamp)
                .header("language", "en-US")
                .header("Content-Type", "application/json")
                .get()
                .build();

        // 3. Отправляем запрос
        custom.info("Отправляем запрос");

        try {
            return client.newCall(request).execute();
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки запроса в BitUnix: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки запроса: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    private Response sendToBitUnixWithAuth(String path, String api, String sec, Map<String, String> data) throws Exception {
        String nonce = generateNonce();
        String timestamp = String.valueOf(System.currentTimeMillis());
        String method = "GET";
        String sign = generateSignApi(api, sec, nonce, timestamp, method, data);

        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
        Request request = new Request.Builder()
                .url(BITUNIX_API_BASE_URL + path)
                .header("api-key", api)
                .header("sign", sign)
                .header("nonce", nonce)
                .header("timestamp", timestamp)
                .header("language", "en-US")
                .header("Content-Type", "application/json")
                .get()
                .build();

        // 3. Отправляем запрос
        custom.info("Отправляем запрос");

        try {
            return client.newCall(request).execute();
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки запроса в BitUnix: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки запроса: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public BigDecimal getEntryPrice(String pair) {
        try {
            JSONArray array = getResponseArray("https://fapi.bitunix.com/api/v1/futures/market/tickers?symbols=" + pair);
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject leverage = array.getJSONObject(i);
                    if (leverage.has("markPrice") && !leverage.isNull("markPrice")) {
                        return new BigDecimal(leverage.getString("markPrice"));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("e: ", e);
        }
        return null;
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
        logger.info("-------------------------------------Cancel limits----------------------------------------------");
        List<OrderResult> results = new ArrayList<>();
        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());

        String nonce = generateNonce();
        String timestamp = String.valueOf(System.currentTimeMillis());
        String method = "POST";
        logger.info("Sign generates");

        List<String> bds = ids.stream().map(s -> String.format("{\"orderId\":\"%s\"}", s)).toList();
        String body = String.format("""
                {
                "symbol": "%2s",
                "orderList": %2s
                }
                """, pair, bds);

        System.err.println(pair + " - PAIR");
        System.out.println(body);

        String sign;
        try {
            sign = generateSignApi(api, secret, nonce, timestamp, method, body);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String path = BITUNIX_API_BASE_URL + "/api/v1/futures/trade/cancel_orders";

        // 2. Формируем запрос
        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
        Request request = new Request.Builder()
                .url(path)
                .post(RequestBody.create(body, MediaType.get("application/json")))
                .addHeader("api-key", api)
                .addHeader("sign", sign)
                .addHeader("nonce", nonce)
                .addHeader("timestamp", timestamp)
                .addHeader("language", "en-US")
                .addHeader("Content-Type", "application/json")
                .build();

        // 3. Отправляем запрос
        custom.info("Отправляем запрос");

        try (Response response = client.newCall(request).execute()) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            custom.info("RESPONSE DATA:");
            custom.warn(responseBody);
            if (validateJsonResopnse(responseBody)) {
                results.add(OrderResult.ok("ордер отменен", "none", pair));
            } else {
                results.add(OrderResult.error("no success order canceling", "123", pair));
            }
        } catch (IOException e) {
            logger.error("Произошла ошибка отправки post запроса в BitGet: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка отправки post запроса: ", e);
            throw new ApiException(e.getMessage());
        }
        logger.info("------------------------------------------------------------------------------------");
        return results;
    }


    @Override
    public List<Order> getOrders(UserEntity user) {
        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());
        String end = "/api/v1/futures/trade/get_pending_orders";
        List<Order> orders = new ArrayList<>();

        try {
            JSONArray array = getAuthResponseArray("data", end, api, secret, "orderList");
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject object = array.getJSONObject(i);
                    boolean isReduce = object.getBoolean("reduceOnly");

                    orders.add(new Order(
                            object.getString("symbol"),                    // symbol
                            new BigDecimal(object.getString("qty")),      // size (qty)
                            object.getString("orderId"),                  // orderId
                            object.getString("clientId"),                 // clientOid (clientId)
                            new BigDecimal(object.getString("tradeQty")), // filledQty (tradeQty)
                            object.getString("fee"),                      // fee
                            new BigDecimal(object.getString("price")),    // price
                            object.getString("status"),                   // state (status)
                            "limit",    // side (из type)
                            object.getString("effect"),                   // timeInForce (effect)
                            object.getString("realizedPNL"),              // totalProfits (realizedPNL)
                            object.getString("positionMode"), // posSide
                            "USDT",                                       // marginCoin (предполагаем USDT)
                            new BigDecimal(object.optString("tpPrice", "0.0")), // presetTakeProfitPrice
                            new BigDecimal(object.optString("slPrice", "0.0")), // presetStopLossPrice
                            new BigDecimal(object.getString("tradeQty")), // filledAmount (tradeQty)
                            "limit",                     // orderType (type)
                            object.getInt("leverage"),                    // leverage
                            object.getString("marginMode"),               // marginMode
                            isReduce,              // reduceOnly
                            isReduce? "close" : "open",       // tradeSide (из type)
                            object.getString("positionMode"), // holdMode (из positionMode)
                            "API",                   // orderSource (source)
                            String.valueOf(object.getLong("ctime")),      // cTime
                            String.valueOf(object.getLong("mtime"))       // uTime
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
        String end = "/api/v1/futures/position/get_pending_positions";
        List<Position> orders = new ArrayList<>();

        try {
            JSONArray array = getAuthResponseArray(end, api, secret, "data");
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject object = array.getJSONObject(i);
                    orders.add(new Position(
                            object.getString("positionId"),
                            "USDT",
                            object.getString("symbol"),
                            object.getString("side"),
                            null,
                            new BigDecimal(object.getString("avgOpenPrice")),
                            null,
                            new BigDecimal(object.optString("qty", "0.0")),
                            object.getInt("leverage"),
                            new BigDecimal(object.getString("unrealizedPNL")),
                            new BigDecimal("0.0"),//object.get("liqPrice") == null? new BigDecimal("0.0") : new BigDecimal(object.getString("liqPrice")),
                            new BigDecimal(object.getString("marginRate")),
                            new BigDecimal(object.getString("avgOpenPrice")),
                            object.getString("ctime"),
                            object.getString("mtime"),
                            new BigDecimal(object.optString("avgOpenPrice", "0.0")),
                            false
                    ));
                }
            }
        } catch (NetworkException e) {
            logger.error("Error:", e);
        }
        return orders;
    }

    public void setupTP(Signal signal, UserEntity user, BigDecimal positionSize, BigDecimal stopLoss, String posId, BigDecimal margin, SymbolInfo symbolInfo, BitUnixWS ws, OrderExecutionContext context) {
        StopInProfitTrigger trigger = context.getTrigger();
        try {
            logger.info("Setuping takes: {}", signal);
            BitUnixStopLossTrailer trailer = new BitUnixStopLossTrailer(this, trigger);
            BitUnixTakesSetuper setuper = new BitUnixTakesSetuper(trigger, this, trailer);

            printPart("setuping take profits");
            List<TakeProfitLevel> tpLevels = BeerjUtils.adjustTakeProfits(signal, positionSize, settingsService.getTPRationsByGroup(user.getGroup()), getEntryPrice(signal.getSymbol()), symbolInfo);
            if (tpLevels.size()-1 <= trigger.getTakeToTrailNumber()) {
                trigger.setTakeToTrailNumber(Math.max(tpLevels.size()-2, 0));
            }

            // 7. Выставляем тейки
            List<Map<String, String>> orders = new ArrayList<>();
            for (TakeProfitLevel level : tpLevels) {
                try {
                    Map<String, String> payload = new HashMap<>();
                    payload.put("qty", level.getSize().toPlainString());
                    payload.put("price", level.getPrice().toPlainString());
                    payload.put("side", signal.getDirection().equalsIgnoreCase("long") ? "BUY" : "SELL");
                    payload.put("tradeSide", "CLOSE");
                    payload.put("positionId", posId);
                    payload.put("orderType", "LIMIT");
                    payload.put("effect", "GTC");
                    payload.put("reduceOnly", "true");


                    payload.put("tpPrice", level.getPrice().toPlainString()); // Цена активации
                    payload.put("tpOrderPrice", level.getPrice().toPlainString()); // Цена исполнения
                    payload.put("tpOrderType", "MARKET");
                    payload.put("tpStopType", "MARK_PRICE");

                    // ПРАВИЛЬНОЕ указание стоп-лосса:
                    payload.put("slPrice", stopLoss.toPlainString());
                    payload.put("slOrderType", "MARKET"); // Или "LIMIT" если нужна цена
                    payload.put("slStopType", "MARK_PRICE");

                    System.out.println("\n\n");
                    custom.blue("Payload:");
                    custom.warn(objectMapper.writeValueAsString(payload));
                    System.out.println("\n\n");

                    orders.add(payload);
                } catch (Exception e) {
                    logger.warn("Failed to place TP at {}: {}", level.getPrice(), e.getMessage());
                }
            }
            List<OrderResult> results = placeOrders(user, signal.getSymbol(), orders);
            setuper.manageTakesInMonitor(ws, signal.getSymbol(), user, results, context.getStopLossId(), tpLevels, symbolInfo, signal.getDirection());
        } catch (Exception e) {
            logger.error("Critical error in setupTP for user {}: {}", user.getTgId(), e.getMessage(), e);
        }
        printPart("take-profits placed");
    }

    private List<OrderResult> placeOrders(UserEntity user, String pair, List<Map<String, String>> orders) {
        List<OrderResult> results = new ArrayList<>();
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("symbol", pair);
            payload.put("orderList", orders);
            System.out.println("PAYLOAD: " + objectMapper.writeValueAsString(payload));

            // Отправляем запрос
            String response = postToBitUnix("/api/v1/futures/trade/batch_order", user, objectMapper.writeValueAsString(payload));
            custom.warn(response);

            JsonObject responseObject = new Gson().fromJson(response, JsonObject.class);
            JsonObject dataObject = responseObject.getAsJsonObject("data");
            JsonArray successList = dataObject.getAsJsonArray("successList");
            successList.forEach(el -> {
                JsonObject item = el.getAsJsonObject();
                if (item.has("orderId"))
                    results.add(OrderResult.ok("Order placed!", item.get("orderId").getAsString(), pair));
            });
            JsonArray failureList = dataObject.getAsJsonArray("failureList");
            failureList.forEach(el -> {
                JsonObject item = el.getAsJsonObject();
                if (item.has("errorMsg") && item.has("clientId"))
                    results.add(OrderResult.error(item.get("errorMsg").getAsString(), item.get("clientId").getAsString(), pair));
            });

            custom.warn("Take response: {}", response);

        } catch (Exception e) {
            logger.error("Failed to place limit order for user {}: {}", user.getTgId(), e.getMessage());
        }
        logger.info("Place orders result: {}", results);
        return results;
    }

    public String postToBitUnix(String endpoint, UserEntity user, String body) {
        try {
            String nonce = generateNonce();
            String method = "POST";

            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String url = "https://fapi.bitunix.com" + endpoint;

            // 2. Создаём подпись (только для приватных запросов)
            String timestamp = String.valueOf(System.currentTimeMillis());

            String sign = generateSignApi(api, secret, nonce, timestamp, method, body);
            String payload = timestamp + method + endpoint + body;

            OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                    .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("api-key", api)
                    .addHeader("sign", sign)
                    .addHeader("nonce", nonce)
                    .addHeader("timestamp", timestamp)
                    .addHeader("language", "en-US")
                    .addHeader("Content-Type", "application/json")
                    .post(RequestBody.create(body, MediaType.get("application/json")))
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

        } catch (Exception e) {
            throw new ApiException("Request failed to " + endpoint);
        }
    }


    private Map<String, String> placeOrder(
            UserEntity user,
            String pair,
            String side,
            BigDecimal size,
            String orderType,
            BigDecimal price) {
        custom.blue(size + " - order size");

        String type = orderType.toUpperCase();
        // Подготавливаем тело запроса
        Map<String, String> payload = new HashMap<>();
        payload.put("symbol", pair);
        payload.put("qty", size.toString());
        payload.put("price", price.toString());
        payload.put("side", side.equalsIgnoreCase("long") ? "BUY" : "SELL");
        payload.put("tradeSide", "OPEN");
        payload.put("orderType", type);
        payload.put("effect", "GTC");


        return payload;
    }

    private void scheduleLimitOrderTimeout(UserEntity user, String pair) {
        try (ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)) {
            scheduler.schedule(() -> {
                try {
                    List<Order> activeLimits = getOrders(user).stream()
                            .filter(e -> e.getOrderType().equals("limit") && e.getState().equals("live")).toList();

                    for (Order order : activeLimits) {
                        boolean cancelled = closeOrder(user, order).succes();
                        if (cancelled) {
                            logger.info("Лимитный ордер по {} отменён (15 мин).", pair);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed to cancel limit order by timeout", e);
                }
            }, Duration.ofMinutes(15).toMinutes(), TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public String getFutureAccountMargin(UserEntity entity) {
        try (HttpClient httpClient = HttpClient.newBuilder().build()) {
            String url = "https://fapi.bitunix.com/api/v1/futures/account";
            String apiKey = security.decrypt(entity.getApiKey());
            String secretKey = security.decrypt(entity.getSecretKey());

            String nonce = generateNonce();
            String timestamp = String.valueOf(System.currentTimeMillis());
            String method = "GET";

            // Данные с параметром marginCoin
            Map<String, String> data = new HashMap<>();
            data.put("marginCoin", "USDT");

            String sign = generateSignApi(apiKey, secretKey, nonce, timestamp, method, data);

            // Формируем query string для URL
            String queryString = data.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining("&"));

            String fullUrl = url + "?" + queryString;
            // Вывод для отладки
            System.out.println("Headers:");
            System.out.println("api-key: " + apiKey);
            System.out.println("nonce: " + nonce);
            System.out.println("timestamp: " + timestamp);
            System.out.println("sign: " + sign);
            System.out.println("URL: " + fullUrl);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(fullUrl))
                    .header("api-key", apiKey)
                    .header("nonce", nonce)
                    .header("timestamp", timestamp)
                    .header("sign", sign)
                    .header("language", "en-US")
                    .header("Content-Type", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            System.out.println("Response status: " + response.statusCode());
            return response.body();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return "";
    }

    @Override
    public List<PositionsModel> getHistoryPositions(UserEntity user) {
        String api = security.decrypt(user.getApiKey());
        String secret = security.decrypt(user.getSecretKey());
        List<PositionsModel> orders = new ArrayList<>();

        try {
            JSONArray array = getAuthResponseArray("data", "/api/v1/futures/position/get_history_positions", api, secret, "positionList");
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    System.err.println("Parse new history positoin");
                    JSONObject object = array.getJSONObject(i);
                    BigDecimal total = new BigDecimal(object.getString("realizedPNL"));
                    BigDecimal fee = new BigDecimal(object.getString("fee"));
                    BigDecimal funding = new BigDecimal(object.getString("funding"));
                    BigDecimal totalReady = total.subtract(fee).subtract(funding);

                    orders.add(new PositionsModel(
                            object.getString("positionId"),
                            object.getString("symbol"),
                            object.getString("side"),
                            null,
                            String.valueOf(object.getInt("leverage")),
                            object.getString("realizedPNL"),
                            object.getString("entryPrice"),
                            object.getString("closePrice"),
                            "close",
                            totalReady.toPlainString()
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
            String api = security.decrypt(user.getApiKey());
            String secret = security.decrypt(user.getSecretKey());
            String nonce = generateNonce();
            String timestamp = String.valueOf(System.currentTimeMillis());
            String method = "POST";
            String path = "/api/v1/futures/account/change_leverage";

            Map<String, String> orderMap = new HashMap<>();
            orderMap.put("symbol", pair);
            orderMap.put("leverage", String.valueOf(leverage));
            orderMap.put("marginCoin", "USDT");

            String body = objectMapper.writeValueAsString(orderMap);
            custom.blue(body);


            String sign = generateSignApi(api, secret, nonce, timestamp, method, body);

            // 2. Формируем запрос
            OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(10, TimeUnit.SECONDS)
                    .readTimeout(10, TimeUnit.SECONDS).writeTimeout(10, TimeUnit.SECONDS).build();
            Request request = new Request.Builder()
                    .url(BITUNIX_API_BASE_URL + path)
                    .post(RequestBody.create(body, MediaType.get("application/json")))
                    .addHeader("api-key", api)
                    .addHeader("sign", sign)
                    .addHeader("nonce", nonce)
                    .addHeader("timestamp", timestamp)
                    .addHeader("language", "en-US")
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
            logger.error("Произошла ошибка смены плеча в BitUnix: {}", e.getMessage());
            throw new ApiException(e.getMessage());
        } catch (Exception e) {
            logger.error("Произошла критичиская ошибка смены плеча в BitUnix: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public void startPositionMonitor(UserEntity user, String symbol, Signal signal, BigDecimal size, BitUnixWS ws, BigDecimal margin, SymbolInfo info, OrderExecutionContext context) {
        Thread.startVirtualThread(() -> {
            ws.setSymbol(symbol);
            try {
                ws.addPositionListener(symbol, position -> {
                    custom.info("Position monitor worked.");
                    if (!context.isPositioned()) {
                        placeStopLoss(user, position, signal.getStopLoss(), info, context);
                        ws.setStopId(position.getPosId());
                        custom.info("Setuped sl: {}", context.getStopLossId());

                        setupTP(signal, user, size, new BigDecimal(signal.getStopLoss()), position.getPosId(), margin, info, ws, context);
                        custom.info("Setuped tp");
                        context.setPositioned(true);
                    }
                });
            } catch (Exception e) {
                logger.error("WebSocket ERROR: ", e);
            }
            ws.startMonitoring();
        });
    }

    @Override
    public List<Ticker> getAllTickers() {
        List<Ticker> tickers = new ArrayList<>();
        OkHttpClient client = new OkHttpClient().newBuilder().connectTimeout(100, TimeUnit.SECONDS)
                .readTimeout(100, TimeUnit.SECONDS).writeTimeout(100, TimeUnit.SECONDS).build();
        Request request = new Request.Builder()
                .url(BITUNIX_API_BASE_URL + "/api/v1/futures/market/tickers")
                .build();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = Objects.requireNonNull(response.body()).string();

            JSONObject ret = new JSONObject(responseBody);
            JSONArray data = ret.getJSONArray("data");

            for (int i = 0; i < data.length(); i++) {
                JSONObject ticker = data.getJSONObject(i);
                tickers.add(new Ticker(ticker.getString("symbol"), new BigDecimal(ticker.optString("lastPrice", "0.0")), new BigDecimal(ticker.optString("markPrice", "0.0"))));
            }
        } catch (Exception e) {
            logger.error("ERR ", e);
        }

        return tickers;
    }

    @Override
    public SymbolInfo getSymbolInfo(UserEntity user, String symbol) {
        try {
            JSONArray array = getResponseArray("https://fapi.bitunix.com/api/v1/futures/market/trading_pairs?symbols=" + symbol);
            JSONObject object = Objects.requireNonNull(array).getJSONObject(0);

            return new SymbolInfo(symbol, object.optString("base", symbol.replace("USDT", "")), Integer.parseInt(object.optString("quotePrecision", "1")), Integer.parseInt(object.optString("basePrecision", "1")), new BigDecimal(object.getString("minTradeVolume")), Integer.parseInt(object.optString("maxLeverage", "1")), new BigDecimal("0.0001"));
        } catch (Exception e) {
            logger.error("SymbolInfo getting error: ", e);
        }
        return new SymbolInfo();
    }

    @Override
    public OrderResult modifyOrder(UserEntity user, Map<String, String> payload) {
        printPart("updating order");
        System.out.println("\n\n");
        String pair = "";

        try {

            String response = postToBitUnix("/api/v1/futures/trade/modify_order", user, objectMapper.writeValueAsString(payload));
            custom.blue("{}", payload);
            custom.warn(response);
            // 4. Парсим ответ
            JsonNode root = objectMapper.readTree(response);
            if (!validateJsonResopnse(response)) {
                logger.warn("Order not modified");
            } else {
                String oId = root.path("data").path("orderId").asText();
                logger.info("Order modified success: {}", oId);

                return OrderResult.ok("Order modified", oId, pair);
            }

        } catch (Exception e) {
            logger.error("Order modifiing failed for symbol {}: {}", pair, e.getMessage());
            return OrderResult.error(e.getMessage(), "none", pair);
        }
        return OrderResult.no();
    }

    @Override
    public void setMarginMode(UserEntity user, String mode, String symbol) {
        printPart("CHANGING MARGIN MODE");
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("marginMode", mode.toUpperCase());
            payload.put("symbol", symbol);
            payload.put("marginCoin", "USDT");

            String payloadJson = objectMapper.writeValueAsString(payload);
            logger.info("Payload to modify margin mode formed: {}", payloadJson);
            String response = postToBitUnix("/api/v1/futures/account/change_margin_mode", user, payloadJson);
            custom.warn(response);
            boolean isValid = validateJsonResopnse(response);
            if (isValid) {
                custom.info("Margin mode changed successfuly");
            } else {
                custom.error("Margin mode not changed");
            }
        } catch (Exception e) {
            logger.error("Margin-mode change error: ", e);
        }
        printPart("margin mode changed");
    }
}