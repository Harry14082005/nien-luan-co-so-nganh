package com.hethongtrongbanking.nienluancosonganh;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * ================================================================
 * FRAUD DETECTION JOB (Flink) — TẦNG 1: Rule cứng
 * ================================================================
 *
 * PHÂN VAI RÕ RÀNG trong hệ thống:
 *
 *   Flink (class này)             Spring Boot Consumer
 *   ─────────────────             ────────────────────
 *   TẦNG 1 - Rule cứng       vs   TẦNG 2 - AI/ML
 *   • Velocity check               • Gọi Python AI Service
 *   • Duplicate check              • Tính risk score
 *   • Large amount check           • APPROVED / UNDER_REVIEW / BLOCKED
 *   → BLOCKED ngay (< 1ms)        → Update DB trực tiếp
 *   → PATCH DB qua HTTP            → Tạo FraudCase
 *
 * Tại sao Flink phù hợp tầng 1?
 *   ListState lưu lịch sử per-card → velocity/duplicate cần tracking
 *   keyBy(ccNum) → cùng thẻ, cùng instance → state chính xác
 *   Xử lý < 1ms → không cần gọi AI cho case rõ ràng
 *
 * Tại sao KHÔNG gọi AI ở đây?
 *   Tránh trùng lặp với Spring Consumer (cùng message, 2 group-id khác nhau)
 *   AI đã có Spring Consumer lo riêng ở tầng 2
 * ================================================================
 */
public class FraudDetectionJob {

    // ════════════════════════════════════════════════════════════
    // TẦNG 1: 3 RULE CỨng - BẮT NGAY (BLOCKED)
    // ════════════════════════════════════════════════════════════
    private static final int    VELOCITY_THRESHOLD    = 1;      // > 1 GD/phút = HIGH_VELOCITY
    private static final double LARGE_AMOUNT_THRESHOLD = 5_000.0; // >= 5000$ = HIGH_AMOUNT
    private static final long   DUPLICATE_WINDOW_MS   = 5_000L;  // Cùng tiền trong 5s = DUPLICATE
    
    private static final String SPRING_BOOT_URL = "http://localhost:8080/api/v1/payments";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ObjectMapper mapper = new ObjectMapper();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("payment_transactions")
                .setGroupId("flink-fraud-group-v5")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<PaymentTransaction> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> {
                    try { return mapper.readValue(json, PaymentTransaction.class); }
                    catch (Exception e) {
                        System.err.println("❌ Parse JSON lỗi: " + e.getMessage());
                        return null;
                    }
                })
                .filter(t -> t != null && t.getCcNum() != null && t.getAmt() > 0);

        stream
                .keyBy(PaymentTransaction::getCcNum)
                .process(new Layer1RuleFunction())
                .print("⛔ TẦNG 1 BLOCK");

        env.execute("Fraud Detection - Layer 1 Rules Only");
    }

    // ================================================================
    // TẦNG 1: CHỈ RULE CỨNG — không gọi AI
    // ================================================================
    public static class Layer1RuleFunction
            extends KeyedProcessFunction<String, PaymentTransaction, String> {

        private ListState<Long>   recentTimestamps;
        private ListState<Double> recentAmounts;
        private transient HttpClient httpClient;

        @Override
        public void open(Configuration parameters) {
            recentTimestamps = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recent-timestamps", Long.class));
            recentAmounts = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recent-amounts", Double.class));
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(3))
                    .build();
        }

        @Override
        public void processElement(PaymentTransaction tx, Context ctx, Collector<String> out)
                throws Exception {

            long nowMs    = System.currentTimeMillis();
            long cutoffMs = nowMs - 60_000L;

            // Đọc và lọc lịch sử trong 1 phút gần nhất
            List<Long>   allTs  = new ArrayList<>();
            List<Double> allAmt = new ArrayList<>();
            for (Long   t : recentTimestamps.get()) allTs.add(t);
            for (Double a : recentAmounts.get())    allAmt.add(a);

            List<Long>   validTs  = new ArrayList<>();
            List<Double> validAmt = new ArrayList<>();
            for (int i = 0; i < allTs.size(); i++) {
                if (allTs.get(i) >= cutoffMs) {
                    validTs.add(allTs.get(i));
                    if (i < allAmt.size()) validAmt.add(allAmt.get(i));
                }
            }

            // ── Rule 1: DUPLICATE ────────────────────────────────────
            // Cùng thẻ + cùng số tiền trong 5 giây = DUPLICATE_TRANSACTION
            long dupCutoff = nowMs - DUPLICATE_WINDOW_MS;
            for (int i = 0; i < validTs.size(); i++) {
                if (validTs.get(i) >= dupCutoff
                        && i < validAmt.size()
                        && Math.abs(validAmt.get(i) - tx.getAmt()) < 0.01) {

                    String fraudType = "DUPLICATE_TRANSACTION";
                    String reason = String.format(
                            "Loại: %s | Cùng thẻ + $%.2f trong %ds",
                            fraudType, tx.getAmt(), DUPLICATE_WINDOW_MS / 1000);
                    blockTransaction(tx, fraudType, reason, out);
                    updateState(validTs, validAmt, nowMs, tx);
                    return;
                }
            }

            // ── Rule 2: VELOCITY ─────────────────────────────────────
            // Quá nhiều giao dịch trong 1 phút = HIGH_VELOCITY
            int txCount = validTs.size() + 1;
            if (txCount > VELOCITY_THRESHOLD) {
                String fraudType = "HIGH_VELOCITY";
                String reason = String.format(
                        "Loại: %s | %d lần/phút (max=%d)",
                        fraudType, txCount, VELOCITY_THRESHOLD);
                blockTransaction(tx, fraudType, reason, out);
                updateState(validTs, validAmt, nowMs, tx);
                return;
            }

            // ── Rule 3: LARGE AMOUNT ─────────────────────────────────
            // Số tiền >= 5000$ = HIGH_AMOUNT
            if (tx.getAmt() >= LARGE_AMOUNT_THRESHOLD) {
                String fraudType = "HIGH_AMOUNT";
                String reason = String.format(
                        "Loại: %s | $%.2f >= ngưỡng $%.0f",
                        fraudType, tx.getAmt(), LARGE_AMOUNT_THRESHOLD);
                blockTransaction(tx, fraudType, reason, out);
                updateState(validTs, validAmt, nowMs, tx);
                return;
            }

            // ── Pass tầng 1 → Spring Consumer sẽ lo tầng 2 (AI) ─────
            System.out.printf("✅ TẦNG 1 PASS | Thẻ: %s | $%.2f | %d GD/phút%n",
                    mask(tx.getCcNum()), tx.getAmt(), txCount);
            updateState(validTs, validAmt, nowMs, tx);
        }

        /**
         * blockTransaction(): BLOCKED tầng 1.
         * Gọi PATCH DB ngay lập tức với fraudType và reason.
         * Spring Consumer vẫn nhận cùng message nhưng khi thấy
         * status đã = BLOCKED → chỉ gọi AI lấy score để tạo FraudCase,
         * không update status nữa (tránh ghi đè).
         */
        private void blockTransaction(PaymentTransaction tx, String fraudType, 
                                      String reason, Collector<String> out) {
            updateStatusInDB(tx.getId(), "BLOCKED", fraudType, reason);

            String alert = String.format(
                    "\n╔════════════════════════════════════════════════╗" +
                            "\n║  🚨 FRAUD DETECTED — TẦNG 1 BLOCKED              ║" +
                            "\n╠════════════════════════════════════════════════╣" +
                            "\n║  Loại gian lận: %-30s ║" +
                            "\n║  Thẻ           : %s                        ║" +
                            "\n║  Số tiền       : $%-10.2f                    ║" +
                            "\n║  Danh mục      : %-25s   ║" +
                            "\n║  Lý do         : %-40s║" +
                            "\n╚════════════════════════════════════════════════╝",
                    fraudType,
                    mask(tx.getCcNum()), tx.getAmt(),
                    tx.getCategory() != null ? tx.getCategory() : "N/A",
                    reason);

            System.out.println(alert);
            out.collect(String.format("[TẦNG 1 BLOCKED] %s | Thẻ %s | $%.2f | %s",
                    fraudType, mask(tx.getCcNum()), tx.getAmt(), reason));
        }

        private void updateStatusInDB(Long id, String status, String fraudType, String reason) {
            if (id == null) return;
            try {
                String body = String.format(
                        "{\"status\":\"%s\",\"fraudType\":\"%s\",\"reason\":\"%s\"}",
                        status, escapeJson(fraudType), escapeJson(reason));

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(SPRING_BOOT_URL + "/" + id + "/status"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(3))
                        .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
                        .build();

                HttpResponse<String> resp = httpClient.send(
                        req, HttpResponse.BodyHandlers.ofString());
                System.out.printf("🔄 DB update | ID=%d → %s (%s) | HTTP %d%n",
                        id, status, fraudType, resp.statusCode());

            } catch (Exception e) {
                System.err.printf("⚠️  PATCH thất bại | ID=%d | %s%n",
                        id, e.getClass().getSimpleName());
            }
        }

        private void updateState(List<Long> ts, List<Double> amt,
                                 long nowMs, PaymentTransaction tx) throws Exception {
            ts.add(nowMs);
            amt.add(tx.getAmt());
            recentTimestamps.update(ts);
            recentAmounts.update(amt);
        }

        private String mask(String ccNum) {
            if (ccNum == null) return "****";
            // Tránh scientific notation: lấy 4 ký tự cuối của chuỗi số thực
            String clean = ccNum.contains(".") ? ccNum.substring(0, ccNum.indexOf(".")) : ccNum;
            if (clean.length() < 4) return "****";
            return "****" + clean.substring(clean.length() - 4);
        }

        private String escapeJson(String s) {
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}