package com.hethongtrongbanking.nienluancosonganh;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface FraudCaseRepository extends JpaRepository<FraudCase, Long> {

    // ── Tìm kiếm cơ bản ──────────────────────────────────────────

    // Tìm case theo transaction ID — dùng để kiểm tra GD đã có case chưa
    Optional<FraudCase> findByTransactionId(Long transactionId);

    // Lấy tất cả case theo status — dashboard dùng để hiển thị danh sách
    List<FraudCase> findByStatusOrderByCreatedAtDesc(FraudCaseStatus status);

    // Lấy các case có risk score cao nhất, chưa xử lý — ưu tiên xét duyệt trước
    List<FraudCase> findByStatusOrderByRiskScoreDesc(FraudCaseStatus status);

    // ── Thống kê cho Dashboard ────────────────────────────────────

    // Đếm số case đang OPEN — hiển thị badge cảnh báo trên dashboard
    long countByStatus(FraudCaseStatus status);

    // Thống kê tổng hợp: số case theo từng status
    // Dùng cho biểu đồ tổng quan trên dashboard
    @Query("SELECT f.status, COUNT(f) FROM FraudCase f GROUP BY f.status")
    List<Object[]> countGroupByStatus();

    // Kiểm tra case đã tồn tại chưa — tránh tạo trùng case cho cùng 1 GD
    boolean existsByTransactionId(Long transactionId);
}