package com.hethongtrongbanking.nienluancosonganh.controller;

import com.hethongtrongbanking.nienluancosonganh.service.IdempotencyService;
import com.hethongtrongbanking.nienluancosonganh.model.Payment;
import com.hethongtrongbanking.nienluancosonganh.service.PaymentService;
import com.hethongtrongbanking.nienluancosonganh.model.TransactionStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/payments")
@Slf4j
public class PaymentController {

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private IdempotencyService idempotencyService;

    /**
     * POST /api/v1/payments
     * Tiếp nhận giao dịch mới, có bảo vệ idempotency.
     *
     * Header tùy chọn:
     *   Idempotency-Key: <uuid>   ← client tự tạo và gửi lên
     *                              nếu không gửi, server tự sinh UUID
     *
     * Response khi trùng lặp (HTTP 200):
     *   Trả về kết quả cũ + header "X-Idempotent-Replayed: true"
     *   HTTP 200 thay vì 4xx vì client không làm gì sai —
     *   đây là hành vi đúng theo chuẩn Stripe/Adyen.
     */
    @PostMapping
    public ResponseEntity<Payment> createPayment(
            @RequestBody Payment payment,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKeyHeader) {

        log.info("📥 Request đến | Thread: {}", Thread.currentThread());

        // Bước 1: Lấy key từ header, hoặc tự sinh nếu client không gửi
        String idempotencyKey = idempotencyService.resolveKey(idempotencyKeyHeader);
        log.info("🔑 Idempotency-Key: {}", idempotencyKey);

        // Bước 2: Kiểm tra key đã xử lý trước đó chưa
        if (idempotencyService.isDuplicate(idempotencyKey)) {
            Payment cachedResult = idempotencyService.getCachedResult(idempotencyKey);

            if (cachedResult != null) {
                log.warn("⚠️  Request trùng lặp! Key={} | Trả về kết quả cũ ID={}",
                        idempotencyKey, cachedResult.getId());

                return ResponseEntity.ok()
                        .header("X-Idempotent-Replayed", "true")
                        .header("X-Idempotency-Key", idempotencyKey)
                        .body(cachedResult);
            }
            // Cache bị mất (Redis restart?) → xử lý lại bình thường
            log.warn("⚠️  Key tồn tại nhưng cache rỗng | Xử lý lại | Key={}", idempotencyKey);
        }

        // Bước 3: Xử lý giao dịch mới
        Payment savedPayment = paymentService.processPayment(payment);

        // Bước 4: Lưu kết quả vào Redis
        idempotencyService.saveResult(idempotencyKey, savedPayment);

        return ResponseEntity.status(HttpStatus.CREATED)
                .header("X-Idempotency-Key", idempotencyKey)
                .body(savedPayment);
    }

    /**
     * GET /api/v1/payments/{id}
     * Client polling endpoint này để biết kết quả cuối sau khi AI chấm xong.
     *
     * Flow:
     *   POST /payments → trả về PENDING ngay
     *   ... (Flink + AI xử lý bất đồng bộ) ...
     *   GET  /payments/{id} → trả về status thực tế (APPROVED / BLOCKED / UNDER_REVIEW)
     *
     * Response 404 nếu không tìm thấy ID.
     */
    @GetMapping("/{id}")
    public ResponseEntity<Payment> getPayment(@PathVariable Long id) {
        return paymentService.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * PATCH /api/v1/payments/{id}/status
     * Flink gọi endpoint này sau khi rule cứng bắt được gian lận.
     * 
     * Request body:
     *   {
     *     "status": "BLOCKED",
     *     "fraudType": "DUPLICATE_TRANSACTION",  (tùy chọn)
     *     "reason": "..."
     *   }
     */
    @PatchMapping("/{id}/status")
    public ResponseEntity<Payment> updateStatus(
            @PathVariable Long id,
            @RequestBody Map<String, String> body) {

        String statusStr = body.get("status");
        String fraudType = body.get("fraudType");
        String reason    = body.get("reason");

        if (statusStr == null) return ResponseEntity.badRequest().build();

        TransactionStatus status;
        try {
            status = TransactionStatus.valueOf(statusStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }

        Payment updated = paymentService.updateStatus(id, status, fraudType, reason);
        return ResponseEntity.ok(updated);
    }
}