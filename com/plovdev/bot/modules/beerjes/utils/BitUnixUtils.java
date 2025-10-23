package com.plovdev.bot.modules.beerjes.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plovdev.bot.modules.beerjes.bitunix.OrderItem;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BitUnixUtils {
    public static final String BITUNIX_API_BASE_URL = "https://fapi.bitunix.com";
    public static final String PAIR_LEVERAGE_ENDPOINT_GET = "/api/v2/mix/market/contracts";
    public static final String BALANCE_ENDPOINT = "/api/v1/futures/account?marginCoin=USDT";
    public static final String PRODUCT_TYPE = "?productType=USDT-FUTURES";
    public static final String GET_POSITIONS_ENDPOINT = "/api/mix/v1/position/allPosition-v2?productType=umcbl&marginCoin=USDT";
    public static final String SYMBOL = "&symbol=";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static String generateNonce() {
        byte[] randomBytes = new byte[32];
        new SecureRandom().nextBytes(randomBytes);
        return Base64.getEncoder().encodeToString(randomBytes);
    }

    // SHA256 хеширование
    private static String sha256Hex(String input) throws NoSuchAlgorithmException {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hash);
    }

    // Конвертация байтов в hex
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append("0");
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    // Генерация подписи
    public static String generateSignApi(String apiKey, String secretKey, String nonce, String timestamp, String method, Map<String, String> data) throws Exception {
        String queryParams = "";
        String body = "";

        if (data != null && !data.isEmpty()) {
            if ("GET".equalsIgnoreCase(method)) {
                // Фильтруем null значения
                Map<String, String> filteredData = data.entrySet().stream()
                        .filter(entry -> entry.getValue() != null)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        ));

                // Сортируем по ключу
                Map<String, String> sortedData = new TreeMap<>(filteredData);

                queryParams = sortedData.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining("&"));

                // Удаляем все не-буквенно-цифровые символы
                queryParams = queryParams.replaceAll("[^a-zA-Z0-9]", "");
            } else if ("POST".equalsIgnoreCase(method)) {
                body = mapper.writeValueAsString(data);
            }
        }

        // Формируем digest_input
        String digestInput = nonce + timestamp + apiKey + queryParams + body;
        System.out.println("Digest input: " + digestInput);

        String digest = sha256Hex(digestInput);
        System.out.println("Digest: " + digest);

        String signInput = digest + secretKey;
        System.out.println("Sign input: " + signInput);

        String sign = sha256Hex(signInput);
        System.out.println("Final sign: " + sign);



        return sign;
    }
    public static String generateSignApi(String apiKey, String secretKey, String nonce, String timestamp, String method, String data) throws Exception {
        // Формируем digest_input
        String digestInput = nonce + timestamp + apiKey + data;
        System.out.println("Digest input: " + digestInput);

        String digest = sha256Hex(digestInput);
        System.out.println("Digest: " + digest);

        String signInput = digest + secretKey;
        System.out.println("Sign input: " + signInput);

        String sign = sha256Hex(signInput);
        System.out.println("Final sign: " + sign);

        return sign;
    }





    public static String generateSign(String nonce, String timestamp, String apiKey,
                                      TreeMap<String, String> queryParamsMap,
                                      String httpBody, String secretKey) {

        StringBuilder queryString = null;
        if (queryParamsMap != null && !queryParamsMap.isEmpty()) {
            queryString = new StringBuilder();
            Set<Map.Entry<String, String>> entrySet = queryParamsMap.entrySet();
            for (Map.Entry<String, String> param : entrySet) {
                if (param.getKey().equals("sign")) {
                    continue;
                }
                String value = param.getValue();

                if (value != null && !value.isEmpty()) {
                    queryString.append(param.getKey());
                    queryString.append(value);
                }
            }
        }
        String baseSignStr = nonce + timestamp + apiKey;
        if (queryString != null) {
            baseSignStr += queryString.toString();
        }
        String digest = encrypt(baseSignStr, httpBody);
        return encrypt(digest + secretKey);
    }
    public static String encrypt(String... args) {

        MessageDigest messageDigest;
        String encodeStr;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            for (String arg : args) {
                if (arg != null && !arg.isEmpty()) {
                    messageDigest.update(arg.getBytes(StandardCharsets.UTF_8));
                }
            }
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        return encodeStr;
    }

    public static String byte2Hex(byte[] bytes) {
        StringBuilder stringBuffer = new StringBuilder();
        String temp;
        for (byte aByte : bytes) {
            temp = Integer.toHexString(aByte & 0xFF);
            if (temp.length() == 1) {
                //1得到一位的进行补0操作
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }

    public static OrderItem parseInput(String content) {
        JSONObject object = new JSONObject(content);
        // Проверяем, есть ли 'data', если нет, пытаемся работать с корнем объекта
        JSONObject data = object.has("data") ? object.getJSONObject("data") : object;

        String oid = data.optString("orderId", "");
        String price = data.optString("price", "0"); // По умолчанию "0" для безопасного парсинга
        String qty = data.optString("qty", "0");   // По умолчанию "0"
        String type = data.optString("type", "");
        String side = data.optString("side", "");
        String status = data.optString("orderStatus", "");
        String posMode = data.optString("positionMode", "");
        boolean reduceOnly = data.optBoolean("reductionOnly", false);

        // --- Безопасное преобразование Integer ---
        int leverage = 0;
        String leverageStr = data.optString("leverage");
        if (leverageStr != null && !leverageStr.isEmpty()) {
            try {
                leverage = Integer.parseInt(leverageStr);
            } catch (NumberFormatException e) {
                // log.warn("Could not parse leverage string: '{}'", leverageStr);
                leverage = 0; // Безопасное значение по умолчанию
            }
        }

        return new OrderItem(oid, price, qty, type, side, status, leverage, posMode, reduceOnly);
    }
}
