package com.hethongtrongbanking.nienluancosonganh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * ================================================================
 * FRAUD TRANSACTION CONSUMER — TẦNG 2: AI/ML
 * ================================================================
 *
 * PHÂN VAI RÕ RÀNG:
 *
 *   Flink (FraudDetectionJob)      Spring Consumer (class này)
 *   ─────────────────────────      ───────────────────────────
 *   TẦNG 1 - Rule cứng        vs   TẦNG 2 - AI/ML
 *   Velocity / Duplicate           Gọi Python AI Service
 *   Large amount                   APPROVED / UNDER_REVIEW / BLOCKED
 *   → BLOCKED ngay                 → Update DB trực tiếp
 *   → PATCH DB qua HTTP            → Tạo FraudCase
 *
 * Xử lý khi Flink đã BLOCKED trước:
 *   Cả 2 nhận cùng message từ Kafka (group-id khác nhau).
 *   Flink nhanh hơn → có thể đã BLOCKED trong DB trước khi
 *   consumer này xử lý xong.
 *
 *   Giải pháp: vẫn gọi AI để lấy score (cần cho FraudCase),
 *   nhưng KHÔNG ghi đè status nếu đã là BLOCKED.
 *   → FraudCase vẫn được tạo đầy đủ với AI score.
 * ================================================================
 */
@Service
@Slf4j
public class FraudTransactionConsumer {

    @Value("${ai.service.url:http://localhost:8000/api/score}")
    private String aiServiceUrl;

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private FraudCaseService fraudCaseService;

    @Autowired
    private PaymentRepository paymentRepository;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private final ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(
            topics   = "payment_transactions",
            groupId  = "fraud-detection-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(Payment payment) {
        log.info("📨 Nhận từ Kafka | ID={} | CC=****{} | ${}",
                payment.getId(),
                payment.getCcNum().substring(payment.getCcNum().length() - 4),
                payment.getAmt());

        try {
            // Bước 1: Gọi AI lấy score — luôn gọi dù Flink đã BLOCKED
            // Cần score để tạo FraudCase đầy đủ thông tin
            AiResult result = callAiService(payment);

            if (result == null) {
                log.warn("⚠️  AI không phản hồi | ID={} | Giữ nguyên status", payment.getId());
                return;
            }

            String reason = String.format(
                    "Score=%.3f [%s] | Patterns=%s",
                    result.riskScore(), result.riskLevel(),
                    result.patterns().isEmpty() ? "none" : result.patterns());

            // Bước 2: Kiểm tra Flink đã xử lý chưa
            // Load lại từ DB để lấy status mới nhất (Flink có thể đã update)
            TransactionStatus currentStatus = getCurrentStatus(payment.getId());

            if (currentStatus == TransactionStatus.BLOCKED) {
                // Flink đã BLOCKED ở tầng 1 → KHÔNG ghi đè status
                // Nhưng vẫn tạo FraudCase để analyst có thể xem xét
                log.info("🔒 Tầng 1 đã BLOCKED | ID={} | AI score={} | Tạo FraudCase",
                        payment.getId(), String.format("%.3f", result.riskScore()));

                // Tìm fraudType từ DB (được set bởi Flink)
                Payment paymentRecord = paymentRepository.findById(payment.getId()).orElse(null);
                String fraudTypeLayer1 = paymentRecord != null ? paymentRecord.getFraudType() : "UNKNOWN";
                
                fraudCaseService.createCase(
                        payment.getId(), 
                        result.riskScore(),
                        fraudTypeLayer1,
                        "LAYER_1",
                        "",
                        "[TẦNG 1] " + reason
                );
                return;
            }

            // Bước 3: Flink chưa xử lý → tầng 2 quyết định status
            TransactionStatus newStatus = decideStatus(result.riskScore());
            paymentService.updateStatus(payment.getId(), newStatus, reason);

            // Bước 4: Tạo FraudCase nếu đáng ngờ
            if (newStatus == TransactionStatus.UNDER_REVIEW
                    || newStatus == TransactionStatus.BLOCKED) {
                fraudCaseService.createCase(
                        payment.getId(),
                        result.riskScore(),
                        result.fraudType(),
                        "LAYER_2",
                        result.patterns(),
                        "[TẦNG 2] " + reason
                );
            }

            String emoji = switch (newStatus) {
                case APPROVED     -> "✅";
                case UNDER_REVIEW -> "⚠️ ";
                case BLOCKED      -> "🚨";
                default           -> "❓";
            };
            log.info("{} [TẦNG 2] ID={} | Score={} | → {}",
                    emoji, payment.getId(),
                    String.format("%.3f", result.riskScore()), newStatus);

        } catch (Exception e) {
            log.error("❌ Lỗi xử lý GD ID={} | {}", payment.getId(), e.getMessage());
        }
    }

    /**
     * getCurrentStatus(): load status mới nhất từ DB.
     * Cần thiết vì Flink có thể đã update BLOCKED sau khi message
     * được push lên Kafka nhưng trước khi consumer này xử lý xong.
     *
     * Trả về PENDING nếu không tìm thấy (an toàn hơn là throw).
     */
    private TransactionStatus getCurrentStatus(Long id) {
        try {
            return paymentRepository.findById(id)
                    .map(Payment::getStatus)
                    .orElse(TransactionStatus.PENDING);
        } catch (Exception e) {
            log.warn("⚠️  Không đọc được status từ DB | ID={}", id);
            return TransactionStatus.PENDING;
        }
    }

    // Ngưỡng khớp với FraudDetectionJob và main.py
    private TransactionStatus decideStatus(double score) {
        if (score >= 0.70) return TransactionStatus.BLOCKED;
        if (score >= 0.35) return TransactionStatus.UNDER_REVIEW;
        return TransactionStatus.APPROVED;
    }

    private AiResult callAiService(Payment payment) {
        try {
            String body = buildAiRequestBody(payment);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(aiServiceUrl))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.warn("⚠️  AI trả về HTTP {} | ID={}", response.statusCode(), payment.getId());
                return null;
            }

            return parseAiResponse(response.body());

        } catch (Exception e) {
            log.warn("⚠️  AI timeout/lỗi | ID={} | {}", payment.getId(), e.getClass().getSimpleName());
            return null;
        }
    }

    private String buildAiRequestBody(Payment payment) {
        double age = calcAge(payment.getDob());
        return String.format(
                "{"
                        + "\"ccNum\":\"%s\","
                        + "\"amount\":%.2f,"
                        + "\"location\":\"%s\","
                        + "\"category\":\"%s\","
                        + "\"merchant\":\"%s\","
                        + "\"lat\":%s,"
                        + "\"lon\":%s,"
                        + "\"merchLat\":%s,"
                        + "\"merchLon\":%s,"
                        + "\"unixTime\":%d,"
                        + "\"cityPop\":%.1f,"
                        + "\"age\":%.1f"
                        + "}",
                escape(payment.getCcNum()),
                payment.getAmt(),
                escape(payment.getLocation()  != null ? payment.getLocation()  : "unknown"),
                escape(payment.getCategory()  != null ? payment.getCategory()  : "misc_net"),
                escape(payment.getMerchant()  != null ? payment.getMerchant()  : "unknown"),
                payment.getLat()      != null ? payment.getLat().toString()      : "0.0",
                payment.getLon()      != null ? payment.getLon().toString()      : "0.0",
                payment.getMerchLat() != null ? payment.getMerchLat().toString() : "0.0",
                payment.getMerchLon() != null ? payment.getMerchLon().toString() : "0.0",
                payment.getUnixTime() != null ? payment.getUnixTime() : System.currentTimeMillis() / 1000L,
                payment.getCityPop()  != null ? (double) payment.getCityPop()   : 0.0,
                age
        );
    }

    private AiResult parseAiResponse(String json) throws Exception {
        JsonNode node    = mapper.readTree(json);
        double riskScore = node.get("riskScore").asDouble();
        String riskLevel = node.get("riskLevel").asText();

        StringBuilder patterns = new StringBuilder();
        JsonNode patternNode   = node.get("patternMatched");
        if (patternNode != null && patternNode.isArray()) {
            patternNode.forEach(p -> {
                if (patterns.length() > 0) patterns.append(", ");
                patterns.append(p.asText());
            });
        }
        
        // Xác định fraud type từ patterns của AI
        String fraudType = "UNKNOWN_PATTERN";
        if (patterns.length() > 0) {
            String[] patternArray = patterns.toString().split(", ");
            if (patternArray.length > 0) {
                fraudType = patternArray[0]; // Lấy pattern đầu tiên làm fraud type
            }
        }
        
        return new AiResult(riskScore, riskLevel, patterns.toString(), fraudType);
    }

    private double calcAge(String dob) {
        if (dob == null || dob.isEmpty()) return 40.0;
        try {
            java.time.LocalDate birth = java.time.LocalDate.parse(dob);
            java.time.LocalDate ref   = java.time.LocalDate.of(2020, 1, 1);
            return java.time.temporal.ChronoUnit.DAYS.between(birth, ref) / 365.0;
        } catch (Exception e) { return 40.0; }
    }

    private String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private record AiResult(double riskScore, String riskLevel, String patterns, String fraudType) {}
}