package com.hethongtrongbanking.nienluancosonganh.service;

import com.hethongtrongbanking.nienluancosonganh.model.FraudCase;
import com.hethongtrongbanking.nienluancosonganh.repository.FraudCaseRepository;
import com.hethongtrongbanking.nienluancosonganh.model.FraudCaseStatus;
import com.hethongtrongbanking.nienluancosonganh.model.TransactionStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class FraudCaseService {

    @Autowired
    private FraudCaseRepository fraudCaseRepository;

    @Autowired
    private PaymentService paymentService;

    // ================================================================
    // TẠO CASE MỚI
    // ================================================================

    /**
     * createCase(): tạo FraudCase mới khi AI phát hiện giao dịch nghi ngờ hoặc Flink bắt được.
     *
     * Được gọi từ:
     *   - FraudTransactionConsumer (TẦNG 2: AI/ML)
     *   - FraudDetectionJob (TẦNG 1: Rule cứng)
     *
     * Nếu case cho transaction này đã tồn tại → bỏ qua, không tạo trùng.
     *
     * @param transactionId   ID giao dịch nghi ngờ
     * @param riskScore       điểm rủi ro từ AI (0.0 – 1.0) hoặc tầng 1
     * @param fraudType       loại gian lận (VD: DUPLICATE_TRANSACTION, HIGH_VELOCITY, HIGH_VALUE_ONLINE, ...)
     * @param detectionLayer  tầng phát hiện: LAYER_1 hoặc LAYER_2
     * @param fraudPatterns   các pattern match (JSON string hoặc comma-separated)
     * @param reason          lý do chi tiết
     * @return FraudCase vừa tạo, hoặc case cũ nếu đã tồn tại
     */
    @Transactional
    public FraudCase createCase(Long transactionId, double riskScore,
                                String fraudType, String detectionLayer,
                                String fraudPatterns, String reason) {
        // Kiểm tra tránh tạo trùng case cho cùng 1 giao dịch
        if (fraudCaseRepository.existsByTransactionId(transactionId)) {
            log.warn("⚠️  Case đã tồn tại cho transaction ID={}, bỏ qua", transactionId);
            return fraudCaseRepository.findByTransactionId(transactionId).orElseThrow();
        }

        FraudCase newCase = FraudCase.builder()
                .transactionId(transactionId)
                .riskScore(riskScore)
                .fraudType(fraudType)
                .detectionLayer(detectionLayer)
                .fraudPatterns(fraudPatterns)
                .reason(reason)
                .status(FraudCaseStatus.OPEN)
                .build();

        FraudCase saved = fraudCaseRepository.save(newCase);
        log.info("🚨 Tạo fraud case | caseId={} | transactionId={} | type={} | layer={} | score={}",
                saved.getCaseId(), transactionId, fraudType, detectionLayer, riskScore);
        return saved;
    }

    /**
     * createCase() overload: dùng cho backward compatibility (2 params từ lâu)
     */
    @Transactional
    public FraudCase createCase(Long transactionId, double riskScore, String reason) {
        return createCase(transactionId, riskScore, "UNKNOWN_TYPE", "LAYER_2", "", reason);
    }

    // ================================================================
    // XỬ LÝ CASE — 3 hành động từ Dashboard
    // ================================================================

    /**
     * approve(): Analyst xác nhận GD hợp lệ → không phải fraud.
     *
     * Hành động:
     *   - FraudCase status → RESOLVED_LEGITIMATE
     *   - Payment status   → APPROVED
     *
     * @param caseId      ID case cần xét duyệt
     * @param analystId   tên/email analyst đang xử lý
     * @param note        ghi chú của analyst (tùy chọn)
     */
    @Transactional
    public FraudCase approve(Long caseId, String analystId, String note) {
        FraudCase fraudCase = getOpenCase(caseId);

        fraudCase.setStatus(FraudCaseStatus.RESOLVED_LEGITIMATE);
        fraudCase.setResolvedBy(analystId);
        fraudCase.setAnalystNote(note);
        fraudCase.setResolvedAt(LocalDateTime.now());

        // Cập nhật payment về APPROVED
        paymentService.updateStatus(
                fraudCase.getTransactionId(),
                TransactionStatus.APPROVED,
                "Analyst " + analystId + " xác nhận hợp lệ"
        );

        FraudCase resolved = fraudCaseRepository.save(fraudCase);
        log.info("✅ Case APPROVED | caseId={} | analyst={} | note={}",
                caseId, analystId, note);
        return resolved;
    }

    /**
     * blockCard(): Analyst xác nhận đúng là gian lận → khóa thẻ.
     *
     * Hành động:
     *   - FraudCase status → RESOLVED_FRAUD
     *   - Payment status   → BLOCKED (giữ nguyên hoặc cập nhật từ UNDER_REVIEW)
     *
     * @param caseId    ID case cần xét duyệt
     * @param analystId tên/email analyst
     * @param note      lý do khóa thẻ
     */
    @Transactional
    public FraudCase blockCard(Long caseId, String analystId, String note) {
        FraudCase fraudCase = getOpenCase(caseId);

        fraudCase.setStatus(FraudCaseStatus.RESOLVED_FRAUD);
        fraudCase.setResolvedBy(analystId);
        fraudCase.setAnalystNote(note);
        fraudCase.setResolvedAt(LocalDateTime.now());

        // Cập nhật payment về BLOCKED
        paymentService.updateStatus(
                fraudCase.getTransactionId(),
                TransactionStatus.BLOCKED,
                "Analyst " + analystId + " xác nhận gian lận: " + (note != null ? note : "")
        );

        FraudCase resolved = fraudCaseRepository.save(fraudCase);
        log.info("🚫 Case BLOCKED | caseId={} | analyst={} | note={}",
                caseId, analystId, note);
        return resolved;
    }

    /**
     * ignore(): Analyst bỏ qua case — thường dùng khi AI báo nhầm rõ ràng.
     *
     * Hành động:
     *   - FraudCase status → IGNORED
     *   - Payment status   → giữ nguyên (không thay đổi)
     *
     * @param caseId    ID case cần bỏ qua
     * @param analystId tên/email analyst
     * @param note      lý do bỏ qua
     */
    @Transactional
    public FraudCase ignore(Long caseId, String analystId, String note) {
        FraudCase fraudCase = getOpenCase(caseId);

        fraudCase.setStatus(FraudCaseStatus.IGNORED);
        fraudCase.setResolvedBy(analystId);
        fraudCase.setAnalystNote(note);
        fraudCase.setResolvedAt(LocalDateTime.now());

        FraudCase resolved = fraudCaseRepository.save(fraudCase);
        log.info("🔕 Case IGNORED | caseId={} | analyst={}", caseId, analystId);
        return resolved;
    }

    // ================================================================
    // QUERY CHO DASHBOARD
    // ================================================================

    /** Lấy tất cả case đang OPEN, sắp xếp theo risk score cao nhất trước */
    public List<FraudCase> getOpenCases() {
        return fraudCaseRepository.findByStatusOrderByRiskScoreDesc(FraudCaseStatus.OPEN);
    }

    /** Lấy tất cả case theo status bất kỳ */
    public List<FraudCase> getCasesByStatus(FraudCaseStatus status) {
        return fraudCaseRepository.findByStatusOrderByCreatedAtDesc(status);
    }

    /**
     * getDashboardStats(): thống kê tổng hợp cho dashboard.
     * Trả về Map gồm: tổng case, số OPEN, số RESOLVED_FRAUD, số RESOLVED_LEGITIMATE, số IGNORED
     *
     * ✅ FIX: Từ 5 query riêng lẻ → 1 query GROUP BY duy nhất.
     *
     * Vấn đề cũ:
     *   fraudCaseRepository.count()              → query 1
     *   fraudCaseRepository.countByStatus(OPEN)  → query 2
     *   ...                                      → query 3, 4, 5
     *   Tổng: 5 round-trips đến DB mỗi lần dashboard load.
     *   Khi hệ thống có traffic cao, dashboard load liên tục → DB bị quá tải.
     *
     * Giải pháp:
     *   countGroupByStatus() dùng JPQL: SELECT f.status, COUNT(f) GROUP BY f.status
     *   → 1 round-trip duy nhất, DB xử lý nội bộ → nhanh hơn đáng kể.
     *   Tính "total" bằng cách cộng tất cả count → không cần query riêng.
     */
    public Map<String, Long> getDashboardStats() {
        // 1 query GROUP BY → trả về List<Object[]> dạng [status, count]
        List<Object[]> rows = fraudCaseRepository.countGroupByStatus();

        // Khởi tạo map với giá trị mặc định = 0
        // Đảm bảo tất cả key luôn tồn tại kể cả khi không có case nào ở status đó
        Map<String, Long> stats = new HashMap<>();
        stats.put("open",               0L);
        stats.put("resolvedFraud",      0L);
        stats.put("resolvedLegitimate", 0L);
        stats.put("ignored",            0L);

        long total = 0L;
        for (Object[] row : rows) {
            FraudCaseStatus status = (FraudCaseStatus) row[0];
            long count             = (Long) row[1];
            total += count;

            // Map từng status → key tương ứng trong response
            switch (status) {
                case OPEN                -> stats.put("open",               count);
                case RESOLVED_FRAUD      -> stats.put("resolvedFraud",      count);
                case RESOLVED_LEGITIMATE -> stats.put("resolvedLegitimate", count);
                case IGNORED             -> stats.put("ignored",            count);
            }
        }

        // Tổng = tổng tất cả count, không cần query riêng
        stats.put("total", total);

        return stats;
    }

    // ================================================================
    // HELPER
    // ================================================================

    /**
     * getOpenCase(): lấy case theo ID, ném lỗi nếu không tìm thấy
     * hoặc case đã được xử lý trước đó.
     */
    private FraudCase getOpenCase(Long caseId) {
        FraudCase fraudCase = fraudCaseRepository.findById(caseId)
                .orElseThrow(() -> new RuntimeException("Không tìm thấy case ID=" + caseId));

        if (fraudCase.getStatus() != FraudCaseStatus.OPEN) {
            throw new RuntimeException(
                    "Case ID=" + caseId + " đã được xử lý (status=" + fraudCase.getStatus() + ")");
        }
        return fraudCase;
    }
}