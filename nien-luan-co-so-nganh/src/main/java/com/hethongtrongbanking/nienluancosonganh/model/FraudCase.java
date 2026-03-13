package com.hethongtrongbanking.nienluancosonganh.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * ================================================================
 * FRAUD CASE - Bảng quản lý các case gian lận cần xét duyệt
 * ================================================================
 *
 * Mối quan hệ với Payment:
 *   1 Payment → 0 hoặc 1 FraudCase
 *   Chỉ tạo FraudCase khi AI score >= 0.35 (UNDER_REVIEW hoặc BLOCKED)
 *   Giao dịch bình thường (APPROVED) không tạo case.
 *
 * Vòng đời của FraudCase:
 *
 *   AI phát hiện (score >= 0.35)
 *       ↓
 *   OPEN  ← case mới tạo, chờ analyst xem xét
 *       ↓
 *   ┌─────────────────────────────────────┐
 *   │ Analyst bấm "Approve"               │
 *   │   → RESOLVED_LEGITIMATE             │  GD hợp lệ, cho qua
 *   │                                     │
 *   │ Analyst bấm "Block Card"            │
 *   │   → RESOLVED_FRAUD                  │  Xác nhận gian lận, khóa thẻ
 *   │                                     │
 *   │ Analyst bấm "Ignore"                │
 *   │   → IGNORED                         │  Bỏ qua, không xử lý
 *   └─────────────────────────────────────┘
 *
 * Dùng cho:
 *   - Dashboard: đếm OPEN cases, hiển thị danh sách chờ xét duyệt
 *   - Audit log: ai duyệt, lúc nào, lý do gì
 *   - Feedback Loop: analyst confirm fraud → AI học lại
 * ================================================================
 */
@Entity
@Table(name = "fraud_cases",
        indexes = {
                // Index để dashboard query nhanh theo status
                @Index(name = "idx_fraud_cases_status",         columnList = "status"),
                // Index để tìm case theo transaction
                @Index(name = "idx_fraud_cases_transaction_id", columnList = "transaction_id"),
                // Index để lọc theo thời gian tạo (dashboard dùng nhiều)
                @Index(name = "idx_fraud_cases_created_at",     columnList = "created_at")
        },
        uniqueConstraints = {
                // ✅ FIX RACE CONDITION: DB tự chặn duplicate ở tầng thấp nhất.
                // Dù Flink và Spring Consumer cùng pass check existsByTransactionId()
                // đồng thời, DB chỉ cho phép 1 trong 2 INSERT thành công.
                // Cái còn lại sẽ ném DataIntegrityViolationException → bắt trong Service.
                @UniqueConstraint(
                        name       = "uq_fraud_cases_transaction_id",
                        columnNames = "transaction_id"
                )
        }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FraudCase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "case_id")
    private Long caseId;

    // FK tới bảng payment — mỗi case gắn với đúng 1 giao dịch
    @Column(name = "transaction_id", nullable = false)
    private Long transactionId;

    // Risk score từ AI Service (0.0 – 1.0)
    @Column(name = "risk_score", nullable = false)
    private Double riskScore;

    // Loại gian lận (ví dụ: DUPLICATE_TRANSACTION, HIGH_VELOCITY, HIGH_AMOUNT, 
    // HIGH_VALUE_ONLINE, CARD_TESTING, GEOGRAPHIC, HIGH_AMOUNT_SUSPICIOUS, NIGHT_UNUSUAL, HIGH_VELOCITY_AI)
    @Column(name = "fraud_type", length = 100)
    private String fraudType;

    // Tầng phát hiện: LAYER_1 (Flink rule) hay LAYER_2 (AI/ML)
    @Column(name = "detection_layer", length = 20)
    private String detectionLayer;

    // Các pattern/dấu hiệu được kích hoạt (mảng, lưu dưới dạng JSON string)
    // Ví dụ: "[HIGH_VALUE_ONLINE, GEOGRAPHIC]"
    @Column(name = "fraud_patterns", length = 500)
    private String fraudPatterns;

    // Lý do AI đưa ra — VD: "Score=0.721 [HIGH] | Patterns: HIGH_VALUE_ONLINE, GEOGRAPHIC"
    @Column(name = "reason", length = 500)
    private String reason;

    // Trạng thái hiện tại của case
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private FraudCaseStatus status;

    // Analyst nào đã xử lý case này (email hoặc username)
    // null nếu chưa có ai xử lý (status = OPEN)
    @Column(name = "resolved_by", length = 100)
    private String resolvedBy;

    // Ghi chú của analyst khi xử lý — VD: "Khách hàng xác nhận giao dịch hợp lệ"
    @Column(name = "analyst_note", length = 500)
    private String analystNote;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    // Thời điểm analyst xử lý xong
    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        if (this.status == null) {
            this.status = FraudCaseStatus.OPEN;
        }
    }
}