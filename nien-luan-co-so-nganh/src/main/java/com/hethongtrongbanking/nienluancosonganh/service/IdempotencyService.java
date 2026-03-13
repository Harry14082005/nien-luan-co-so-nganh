package com.hethongtrongbanking.nienluancosonganh.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hethongtrongbanking.nienluancosonganh.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

/**
 * ================================================================
 * IDEMPOTENCY SERVICE - Chống xử lý trùng giao dịch
 * ================================================================
 *
 * Vấn đề cần giải quyết:
 *   Client gửi POST /payments → timeout → gửi lại
 *   → Hệ thống xử lý 2 lần → trừ tiền 2 lần!
 *
 * Giải pháp:
 *   Mỗi request gắn 1 Idempotency-Key duy nhất (UUID).
 *   Key + kết quả được lưu vào Redis với TTL 24 giờ.
 *   Request tiếp theo cùng key → trả về kết quả cũ, không xử lý lại.
 *
 * Flow:
 *   Request đến
 *       ↓
 *   Có Idempotency-Key trong header?
 *       ├── Không → server tự sinh UUID
 *       ↓
 *   Key đã tồn tại trong Redis?
 *       ├── Có  → trả về kết quả cũ (DUPLICATE, bỏ qua)
 *       └── Không → xử lý bình thường → lưu key + kết quả vào Redis
 *
 * Lưu ý với đồ án:
 *   Redis lưu trong RAM → cực nhanh (< 1ms), phù hợp hệ thống tốc độ cao.
 *   TTL 24h: sau 24 giờ key tự xóa, không cần dọn dẹp thủ công.
 * ================================================================
 */
@Service
@Slf4j
public class IdempotencyService {

    // Prefix để phân biệt key idempotency với các key Redis khác
    // VD: "idempotency:550e8400-e29b-41d4-a716-446655440000"
    private static final String KEY_PREFIX = "idempotency:";

    // TTL mặc định: 24 giờ — đọc từ application.properties
    @Value("${idempotency.ttl-hours:24}")
    private long ttlHours;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())                    // ✅ FIX: hỗ trợ LocalDateTime Java 8+
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // lưu dạng ISO string, không phải số

    // ================================================================
    // SINH KEY
    // ================================================================

    /**
     * resolveKey(): lấy key từ header, hoặc tự sinh UUID nếu client không gửi.
     *
     * @param headerKey giá trị header "Idempotency-Key" (có thể null)
     * @return key sẽ dùng cho request này
     */
    public String resolveKey(String headerKey) {
        if (headerKey != null && !headerKey.isBlank()) {
            return headerKey.trim();
        }
        // Client không gửi key → server tự sinh UUID
        String generated = UUID.randomUUID().toString();
        log.debug("🔑 Tự sinh Idempotency-Key: {}", generated);
        return generated;
    }

    // ================================================================
    // KIỂM TRA TRÙNG LẶP
    // ================================================================

    /**
     * isDuplicate(): kiểm tra key đã tồn tại trong Redis chưa.
     *
     * @param key idempotency key cần kiểm tra
     * @return true nếu đây là request trùng lặp (key đã có trong Redis)
     */
    public boolean isDuplicate(String key) {
        String redisKey = KEY_PREFIX + key;
        Boolean exists  = redisTemplate.hasKey(redisKey);
        return Boolean.TRUE.equals(exists);
    }

    /**
     * getCachedResult(): lấy kết quả cũ đã lưu trong Redis.
     * Chỉ gọi sau khi isDuplicate() trả về true.
     *
     * @param key idempotency key
     * @return Payment object đã xử lý trước đó, null nếu lỗi parse
     */
    public Payment getCachedResult(String key) {
        try {
            String json = redisTemplate.opsForValue().get(KEY_PREFIX + key);
            if (json == null) return null;
            return mapper.readValue(json, Payment.class);
        } catch (Exception e) {
            log.error("❌ Không đọc được cache Redis | key={} | {}", key, e.getMessage());
            return null;
        }
    }

    // ================================================================
    // LƯU KẾT QUẢ
    // ================================================================

    /**
     * saveResult(): lưu key + kết quả vào Redis sau khi xử lý thành công.
     *
     * Chỉ lưu khi xử lý thành công để tránh cache lỗi:
     *   - Nếu lưu kết quả lỗi → request retry hợp lệ bị từ chối oan
     *   - Nếu không lưu → mỗi retry đều được xử lý lại (đúng hành vi mong muốn khi lỗi)
     *
     * @param key     idempotency key
     * @param payment kết quả giao dịch đã xử lý
     */
    public void saveResult(String key, Payment payment) {
        try {
            String json     = mapper.writeValueAsString(payment);
            String redisKey = KEY_PREFIX + key;

            // Lưu với TTL — Redis tự xóa sau ttlHours giờ
            redisTemplate.opsForValue().set(redisKey, json, Duration.ofHours(ttlHours));

            log.info("💾 Lưu idempotency key vào Redis | key={} | TTL={}h | paymentId={}",
                    key, ttlHours, payment.getId());

        } catch (Exception e) {
            // Lỗi lưu Redis không nên làm hỏng response trả về client
            // Chỉ log warning, tiếp tục bình thường
            log.warn("⚠️  Không lưu được Redis | key={} | {}", key, e.getMessage());
        }
    }
}