package com.hethongtrongbanking.nienluancosonganh;

/**
 * ================================================================
 * FRAUD CASE STATUS - Trạng thái xét duyệt của một fraud case
 * ================================================================
 *
 * OPEN                → Case mới, chưa có analyst xem xét
 * RESOLVED_LEGITIMATE → Analyst xác nhận: GD hợp lệ, không phải fraud
 *                       → Payment status cập nhật về APPROVED
 * RESOLVED_FRAUD      → Analyst xác nhận: đúng là gian lận
 *                       → Payment status giữ BLOCKED, thẻ bị khóa
 * IGNORED             → Analyst bỏ qua, không cần xử lý
 *                       → Dùng cho các case AI cảnh báo nhầm rõ ràng
 * ================================================================
 */
public enum FraudCaseStatus {

    OPEN,                  // Chờ analyst xét duyệt
    RESOLVED_LEGITIMATE,   // Xác nhận hợp lệ → cho qua
    RESOLVED_FRAUD,        // Xác nhận gian lận → khóa thẻ
    IGNORED                // Bỏ qua
}