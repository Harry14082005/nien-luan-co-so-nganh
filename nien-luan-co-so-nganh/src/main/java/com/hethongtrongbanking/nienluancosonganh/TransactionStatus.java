package com.hethongtrongbanking.nienluancosonganh;

/**
 * ================================================================
 * TRANSACTION STATUS - Vòng đời của một giao dịch
 * ================================================================
 *
 * Flow chuẩn:
 *
 *   Client gửi request
 *       ↓
 *   PENDING  ←─── Vừa tạo, chờ xử lý
 *       ↓
 *   Flink + AI phân tích
 *       ↓
 *   ┌─────────────────────────────────┐
 *   │ score < 0.35  → APPROVED        │  Giao dịch bình thường, cho qua
 *   │ score 0.35-0.69 → UNDER_REVIEW  │  Nghi ngờ, cần analyst kiểm tra
 *   │ score >= 0.70 → BLOCKED         │  Gian lận rõ ràng, chặn ngay
 *   │ Tang 1 vi phạm → BLOCKED        │  Velocity / Duplicate / Large amount
 *   └─────────────────────────────────┘
 *       ↓
 *   REJECTED  ←─── Analyst từ chối thủ công (từ Case Management)
 *
 * Ý nghĩa với hệ thống:
 *   - Dashboard  : đếm theo status → biết bao nhiêu GD đang PENDING, bị BLOCKED
 *   - Audit log  : truy vết lịch sử thay đổi status của từng GD
 *   - Fraud analyst: lọc UNDER_REVIEW để xem xét thủ công
 *   - Case Management: analyst bấm "Chấp nhận" → APPROVED, "Khóa thẻ" → BLOCKED
 * ================================================================
 */
public enum TransactionStatus {

    PENDING,        // Vừa tạo, chờ fraud detection xử lý
    APPROVED,       // AI chấm score thấp (< 0.35) → giao dịch hợp lệ
    UNDER_REVIEW,   // AI chấm score trung bình (0.35–0.69) → cần analyst kiểm tra
    REJECTED,       // Analyst từ chối thủ công qua Case Management
    BLOCKED         // AI hoặc Rule Tang 1 phát hiện gian lận → chặn ngay
}