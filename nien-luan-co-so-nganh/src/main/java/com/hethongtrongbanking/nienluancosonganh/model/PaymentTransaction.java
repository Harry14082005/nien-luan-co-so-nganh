package com.hethongtrongbanking.nienluancosonganh.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * ================================================================
 * PaymentTransaction - Cấu trúc dữ liệu GD mà Flink đọc từ Kafka
 * ================================================================
 *
 * Vị trí trong hệ thống:
 *   Spring Boot gửi Payment lên Kafka (dạng JSON)
 *   -> Flink đọc JSON đó -> deserialize vào class này
 *   -> FraudDetectionJob dùng class này để xử lý
 *
 * Tên trường trong class này phải KHỚP với tên JSON mà Spring Boot gửi.
 * Spring Boot dùng Payment.java để serialize -> JSON -> Kafka.
 * Flink dùng PaymentTransaction.java để deserialize <- JSON <- Kafka.
 *
 * ✅ FIX DUPLICATE SETTER:
 *   Đặt @JsonProperty trên FIELD thay vì tạo setter thứ 2.
 *   Jackson đọc annotation trên field trực tiếp — không cần setter riêng cho snake_case.
 *   Trước: setCcNum() + setCc_num() → Jackson gọi cả 2, có thể conflict
 *   Sau:   @JsonProperty("cc_num") trên field → 1 điểm map duy nhất, rõ ràng
 *
 * @JsonIgnoreProperties(ignoreUnknown = true):
 *   Nếu JSON có thêm trường nào chưa có ở đây -> bỏ qua, không báo lỗi.
 *   Giúp hệ thống linh hoạt khi Spring Boot thêm trường mới.
 * ================================================================
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaymentTransaction {

    private Long   id;       // ID tự động tăng trong PostgreSQL

    @JsonProperty("cc_num")
    private String ccNum;    // số thẻ tín dụng — khớp với cột "cc_num" trong CSV

    private String location; // tên thành phố giao dịch — khớp với "city" trong CSV
    private String status;   // trạng thái GD: PENDING / APPROVED / BLOCKED

    private Double amt;      // số tiền giao dịch (USD) — khớp với "amt" trong CSV

    @JsonProperty("unix_time")
    private Long   unixTime; // thời gian GD (Unix timestamp, đơn vị giây) — khớp "unix_time"

    private String category; // loại GD: shopping_net, food_dining, ... — khớp "category"
    private String merchant; // tên cửa hàng — khớp "merchant"

    // Tọa độ chủ thẻ (vị trí nhà) — khớp "lat", "long" trong CSV
    private Double lat;
    private Double lon;

    // Tọa độ merchant — khớp "merch_lat", "merch_long" trong CSV
    // Kết hợp với lat/lon → tính distance_km bằng Haversine
    @JsonProperty("merch_lat")
    private Double merchLat;

    @JsonProperty("merch_long")
    private Double merchLon;

    // Dân số thành phố — khớp "city_pop" trong CSV
    // Fraud có xu hướng xảy ra ở thành phố nhỏ vắng người hơn đô thị lớn
    @JsonProperty("city_pop")
    private Long cityPop;

    // Ngày sinh chủ thẻ — khớp "dob" trong CSV (định dạng "yyyy-MM-dd")
    // Dùng để tính tuổi → feature "age" trong mô hình ML
    private String dob;

    public PaymentTransaction() {} // Jackson cần constructor rỗng để deserialize

    // ================================================================
    // GETTERS & SETTERS
    // ✅ Không còn duplicate setter — @JsonProperty đã đặt trên field
    // Jackson tự map "cc_num" → ccNum, "unix_time" → unixTime, v.v.
    // ================================================================

    public Long getId()              { return id; }
    public void setId(Long id)       { this.id = id; }

    public String getCcNum()              { return ccNum; }
    public void setCcNum(String ccNum)    { this.ccNum = ccNum; }

    public Double getAmt()           { return amt != null ? amt : 0.0; }
    public void setAmt(Double amt)   { this.amt = amt; }

    public String getLocation()              { return location; }
    public void setLocation(String location) { this.location = location; }

    public String getStatus()             { return status; }
    public void setStatus(String status)  { this.status = status; }

    public Long getUnixTime()              { return unixTime; }
    public void setUnixTime(Long unixTime) { this.unixTime = unixTime; }

    public String getCategory()              { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getMerchant()              { return merchant; }
    public void setMerchant(String merchant) { this.merchant = merchant; }

    public Double getLat()           { return lat; }
    public void setLat(Double lat)   { this.lat = lat; }

    public Double getLon()           { return lon; }
    public void setLon(Double lon)   { this.lon = lon; }

    public Double getMerchLat()              { return merchLat; }
    public void setMerchLat(Double merchLat) { this.merchLat = merchLat; }

    public Double getMerchLon()              { return merchLon; }
    public void setMerchLon(Double merchLon) { this.merchLon = merchLon; }

    public Long getCityPop()             { return cityPop; }
    public void setCityPop(Long cityPop) { this.cityPop = cityPop; }

    public String getDob()           { return dob; }
    public void setDob(String dob)   { this.dob = dob; }

    /**
     * calcAge(): tính tuổi chủ thẻ từ ngày sinh.
     * Mốc tính: 2020-01-01 (trùng với dữ liệu fraudTest.csv của Kaggle)
     * Xử lý lỗi: nếu dob null, rỗng, sai định dạng → mặc định 40 tuổi
     */
    public double calcAge() {
        if (dob == null || dob.isEmpty()) return 40.0;
        try {
            java.time.LocalDate birth = java.time.LocalDate.parse(dob);
            java.time.LocalDate ref   = java.time.LocalDate.of(2020, 1, 1);
            return java.time.temporal.ChronoUnit.DAYS.between(birth, ref) / 365.0;
        } catch (Exception e) {
            return 40.0;
        }
    }

    /**
     * toString(): in thông tin ngắn gọn để debug.
     */
    @Override
    public String toString() {
        return String.format("Transaction{ccNum='****%s', amount=%.2f, category='%s', merchant='%s', dist=%.1fkm}",
                ccNum != null && ccNum.length() >= 4 ? ccNum.substring(ccNum.length() - 4) : "????",
                getAmt(), category, merchant,
                (lat != null && lon != null && merchLat != null && merchLon != null)
                        ? calcDistanceKm() : 0.0);
    }

    /**
     * calcDistanceKm(): tính khoảng cách nhà → merchant bằng công thức Haversine.
     * Chỉ dùng trong toString() để debug.
     */
    private double calcDistanceKm() {
        if (lat == null || lon == null || merchLat == null || merchLon == null) return 0;
        double R    = 6371;
        double phi1 = Math.toRadians(lat),     phi2 = Math.toRadians(merchLat);
        double dphi = Math.toRadians(merchLat - lat);
        double dlam = Math.toRadians(merchLon - lon);
        double a    = Math.sin(dphi/2) * Math.sin(dphi/2)
                + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dlam/2) * Math.sin(dlam/2);
        return 2 * R * Math.asin(Math.sqrt(a));
    }
}