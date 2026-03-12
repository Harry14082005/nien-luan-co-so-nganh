package com.hethongtrongbanking.nienluancosonganh;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonProperty("cc_num")
    private String ccNum;

    @JsonProperty("amt")
    private Double amt;

    private String merchant;
    private String category;

    @JsonProperty("unix_time")
    private Long unixTime;

    // Tọa độ chủ thẻ (nhà)
    private Double lat;
    private Double lon;

    // Tọa độ merchant — dùng để tính distance_km cho AI
    @JsonProperty("merch_lat")
    private Double merchLat;

    @JsonProperty("merch_long")
    private Double merchLon;

    // Dân số thành phố — feature quan trọng trong Kaggle dataset
    @JsonProperty("city_pop")
    private Long cityPop;

    // Ngày sinh chủ thẻ — để tính tuổi
    private String dob;

    private String location;

    // ================================================================
    // STATUS — vòng đời giao dịch
    // @Enumerated(EnumType.STRING): lưu vào DB dạng chữ ("PENDING",
    // "APPROVED",...) thay vì số (0,1,2,...) để dễ đọc khi query thẳng DB
    // ================================================================
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TransactionStatus status;

    private LocalDateTime createdAt;

    // Thời điểm status thay đổi lần cuối — dùng cho audit log & dashboard
    private LocalDateTime updatedAt;

    // Lý do bị BLOCKED hoặc UNDER_REVIEW — AI Service trả về, lưu để analyst xem
    @Column(length = 500)
    private String statusReason;

    // Loại gian lận (nếu bị bắt) — VD: DUPLICATE_TRANSACTION, HIGH_VELOCITY, HIGH_VALUE_ONLINE, ...
    @Column(length = 100)
    private String fraudType;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        if (this.unixTime == null) {
            this.unixTime = System.currentTimeMillis() / 1000L;
        }
        // Mọi giao dịch mới đều bắt đầu bằng PENDING
        if (this.status == null) {
            this.status = TransactionStatus.PENDING;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        // Tự động cập nhật updatedAt mỗi khi status thay đổi
        this.updatedAt = LocalDateTime.now();
    }
}