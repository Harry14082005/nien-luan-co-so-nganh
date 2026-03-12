package com.hethongtrongbanking.nienluancosonganh;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * ================================================================
 * FRAUD CASE CONTROLLER - REST API cho Dashboard analyst
 * ================================================================
 *
 * Endpoints:
 *
 *   GET  /api/v1/fraud-cases                  → Lấy danh sách OPEN cases
 *   GET  /api/v1/fraud-cases/stats            → Thống kê dashboard
 *   GET  /api/v1/fraud-cases?status=IGNORED   → Lấy case theo status
 *
 *   POST /api/v1/fraud-cases/{id}/approve     → Analyst xác nhận hợp lệ
 *   POST /api/v1/fraud-cases/{id}/block-card  → Analyst xác nhận gian lận
 *   POST /api/v1/fraud-cases/{id}/ignore      → Analyst bỏ qua
 * ================================================================
 */
@RestController
@RequestMapping("/api/v1/fraud-cases")
@Slf4j
public class FraudCaseController {

    @Autowired
    private FraudCaseService fraudCaseService;

    // ================================================================
    // GET — Truy vấn
    // ================================================================

    /**
     * GET /api/v1/fraud-cases
     * Lấy danh sách case, mặc định là OPEN (sắp xếp risk score cao nhất trước).
     * Truyền ?status=RESOLVED_FRAUD để lọc theo status khác.
     *
     * Ví dụ:
     *   GET /api/v1/fraud-cases              → tất cả OPEN cases
     *   GET /api/v1/fraud-cases?status=IGNORED → các case đã bỏ qua
     */
    @GetMapping
    public ResponseEntity<List<FraudCase>> getCases(
            @RequestParam(required = false) FraudCaseStatus status) {

        List<FraudCase> cases = (status == null)
                ? fraudCaseService.getOpenCases()
                : fraudCaseService.getCasesByStatus(status);

        return ResponseEntity.ok(cases);
    }

    /**
     * GET /api/v1/fraud-cases/stats
     * Thống kê tổng hợp cho dashboard.
     *
     * Response:
     * {
     *   "total": 150,
     *   "open": 23,
     *   "resolvedFraud": 87,
     *   "resolvedLegitimate": 34,
     *   "ignored": 6
     * }
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Long>> getStats() {
        return ResponseEntity.ok(fraudCaseService.getDashboardStats());
    }

    // ================================================================
    // POST — Hành động của Analyst
    // ================================================================

    /**
     * POST /api/v1/fraud-cases/{id}/approve
     * Analyst xác nhận GD hợp lệ → không phải fraud.
     *
     * Request body:
     * {
     *   "analystId": "analyst@bank.com",
     *   "note": "Khách hàng xác nhận GD hợp lệ qua hotline"
     * }
     */
    @PostMapping("/{id}/approve")
    public ResponseEntity<?> approve(
            @PathVariable Long id,
            @RequestBody ActionRequest request) {
        try {
            FraudCase resolved = fraudCaseService.approve(id, request.analystId(), request.note());
            return ResponseEntity.ok(resolved);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/v1/fraud-cases/{id}/block-card
     * Analyst xác nhận gian lận → khóa thẻ.
     *
     * Request body:
     * {
     *   "analystId": "analyst@bank.com",
     *   "note": "Phát hiện giao dịch bất thường ở nước ngoài"
     * }
     */
    @PostMapping("/{id}/block-card")
    public ResponseEntity<?> blockCard(
            @PathVariable Long id,
            @RequestBody ActionRequest request) {
        try {
            FraudCase resolved = fraudCaseService.blockCard(id, request.analystId(), request.note());
            return ResponseEntity.ok(resolved);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/v1/fraud-cases/{id}/ignore
     * Analyst bỏ qua case — AI cảnh báo nhầm.
     *
     * Request body:
     * {
     *   "analystId": "analyst@bank.com",
     *   "note": "AI false positive, pattern bình thường của khách hàng này"
     * }
     */
    @PostMapping("/{id}/ignore")
    public ResponseEntity<?> ignore(
            @PathVariable Long id,
            @RequestBody ActionRequest request) {
        try {
            FraudCase resolved = fraudCaseService.ignore(id, request.analystId(), request.note());
            return ResponseEntity.ok(resolved);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // ================================================================
    // REQUEST BODY
    // ================================================================

    /**
     * ActionRequest: body chung cho 3 hành động approve / block-card / ignore.
     * Dùng Java Record — immutable, tự có constructor + getter, không cần Lombok.
     */
    record ActionRequest(String analystId, String note) {}
}