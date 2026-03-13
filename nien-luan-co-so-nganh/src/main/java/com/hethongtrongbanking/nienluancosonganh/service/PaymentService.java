package com.hethongtrongbanking.nienluancosonganh.service;

import com.hethongtrongbanking.nienluancosonganh.exception.ResourceNotFoundException;
import com.hethongtrongbanking.nienluancosonganh.kafka.PaymentProducer;
import com.hethongtrongbanking.nienluancosonganh.model.Payment;
import com.hethongtrongbanking.nienluancosonganh.model.TransactionStatus;
import com.hethongtrongbanking.nienluancosonganh.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PaymentService {

    @Autowired
    private PaymentRepository repository;

    @Autowired
    private PaymentProducer producer;

    /**
     * processPayment(): tiếp nhận giao dịch mới từ Controller.
     *
     * Flow:
     *   1. Set status = PENDING (rõ ràng qua enum, không dùng String cứng)
     *   2. Lưu vào DB → có ID
     *   3. Đẩy vào Kafka → Flink + AI xử lý bất đồng bộ
     *   4. Trả về cho client (status vẫn là PENDING, AI chưa kịp chấm)
     *
     * Lưu ý: AI chấm điểm xong sẽ gọi updateStatus() để cập nhật DB.
     * Client nên polling GET /api/v1/payments/{id} để biết kết quả cuối.
     */
    @Transactional
    public Payment processPayment(Payment payment) {
        log.info("Thread xử lý request: {}", Thread.currentThread());

        // Đặt status ban đầu = PENDING qua enum (type-safe, không sợ typo)
        payment.setStatus(TransactionStatus.PENDING);

        // Lưu vào DB để lấy ID, Kafka cần ID để trace sau
        Payment savedPayment = repository.save(payment);
        log.info("✅ Lưu DB thành công | ID={} | CC=****{} | ${}",
                savedPayment.getId(),
                savedPayment.getCcNum().substring(savedPayment.getCcNum().length() - 4),
                savedPayment.getAmt());

        // Đẩy vào Kafka → Flink + AI Service sẽ xử lý bất đồng bộ
        producer.sendPaymentEvent(savedPayment);

        return savedPayment;
    }

    /**
     * findById(): lấy thông tin giao dịch theo ID.
     * Dùng cho GET /api/v1/payments/{id} — client polling kết quả.
     */
    public java.util.Optional<Payment> findById(Long id) {
        return repository.findById(id);
    }

    /**
     * updateStatus(): được gọi sau khi Flink/AI chấm điểm xong.
     *
     * Ai gọi hàm này?
     *   - FraudDetectionJob (Flink) gọi qua HTTP PATCH sau khi có kết quả AI
     *   - Analyst gọi qua Case Management (APPROVED / REJECTED thủ công)
     *
     * @param id      ID giao dịch cần cập nhật
     * @param status  trạng thái mới (APPROVED / BLOCKED / UNDER_REVIEW / REJECTED)
     * @param reason  lý do (AI trả về hoặc analyst nhập tay), có thể null
     */
    @Transactional
    public Payment updateStatus(Long id, TransactionStatus status, String reason) {
        return updateStatus(id, status, null, reason);
    }

    /**
     * updateStatus() overload: với fraudType từ Flink/AI
     *
     * @param id        ID giao dịch cần cập nhật
     * @param status    trạng thái mới
     * @param fraudType loại gian lận (VD: DUPLICATE_TRANSACTION, HIGH_VELOCITY, ...)
     * @param reason    lý do chi tiết
     */
    @Transactional
    public Payment updateStatus(Long id, TransactionStatus status, String fraudType, String reason) {
        Payment payment = repository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Payment", id)); // ✅ FIX: 404 thay vì 500;

        TransactionStatus oldStatus = payment.getStatus();

        // ✅ FIX: Guard chống ghi đè BLOCKED
        // Flink (tầng 1) BLOCK trước → Spring Consumer (tầng 2) không được ghi đè
        // Ngoại lệ: analyst chủ động APPROVE sau khi review → cho phép đổi từ BLOCKED
        if (oldStatus == TransactionStatus.BLOCKED && status != TransactionStatus.BLOCKED
                && status != TransactionStatus.APPROVED) {
            log.warn("⚠️  Bỏ qua ghi đè BLOCKED | ID={} | Tầng 2 muốn set {} nhưng Tầng 1 đã BLOCKED",
                    id, status);
            return payment;
        }

        payment.setStatus(status);
        payment.setStatusReason(reason);
        if (fraudType != null) {
            payment.setFraudType(fraudType);
        }

        Payment updated = repository.save(payment);
        log.info("🔄 Status thay đổi | ID={} | {} → {} | Fraud Type: {} | Lý do: {}",
                id, oldStatus, status, fraudType != null ? fraudType : "N/A",
                reason != null ? reason : "N/A");

        return updated;
    }
}