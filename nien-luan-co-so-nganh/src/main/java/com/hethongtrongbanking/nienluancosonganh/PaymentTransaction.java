package com.hethongtrongbanking.nienluancosonganh;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * ================================================================
 * PaymentTransaction - Cau truc du lieu GD ma Flink doc tu Kafka
 * ================================================================
 *
 * Vi tri trong he thong:
 *   Spring Boot gui Payment len Kafka (dang JSON)
 *   -> Flink doc JSON do -> deserialize vao class nay
 *   -> FraudDetectionJob dung class nay de xu ly
 *
 * Luu y quan trong:
 *   Ten truong trong class nay phai KHOP voi ten JSON ma Spring Boot gui.
 *   Spring Boot dung Payment.java de serialize -> JSON -> Kafka.
 *   Flink dung PaymentTransaction.java de deserialize <- JSON <- Kafka.
 *   Neu ten truong lech nhau -> gia tri se bi null -> tinh sai.
 *
 *   Vi du: Spring Boot gui "cc_num" -> @JsonProperty("cc_num") o day
 *          de Jackson biet can gan "cc_num" vao bien ccNum.
 *
 * @JsonIgnoreProperties(ignoreUnknown = true):
 *   Neu JSON co them truong nao chua co o day -> bo qua, khong bao loi.
 *   Giup he thong linh hoat khi Spring Boot them truong moi.
 * ================================================================
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class PaymentTransaction {

    private Long    id;      // ID tu dong tang trong PostgreSQL
    private String  ccNum;   // so the tin dung (16 chu so, dang String tranh mat do chinh xac)
    private String  location; // ten thanh pho giao dich
    private String  status;   // trang thai GD: PENDING / APPROVED / REJECTED
    private Double  amt;      // so tien giao dich (USD) - khop voi "amt" trong CSV va JSON
    private Long    unixTime; // thoi gian GD (Unix timestamp, don vi giay)
    private String  category; // loai GD: shopping_net, food_dining, health_fitness,...
    private String  merchant; // ten cua hang / dich vu

    // Toa do chu the (vi tri nha cua nguoi so huu the)
    // Dung de tinh khoang cach den noi giao dich (distance_km)
    private Double lat; // vi do

    private Double lon; // kinh do

    // Toa do merchant (vi tri cua hang/dich vu)
    // Ket hop voi lat/lon cua chu the -> tinh distance_km bang cong thuc Haversine
    // distance_km la feature quan trong nhat trong mo hinh phat hien fraud
    @JsonProperty("merch_lat")
    private Double merchLat;

    @JsonProperty("merch_long")
    private Double merchLon;

    // Dan so thanh pho noi giao dich xay ra
    // Fraud co xu huong xay ra o thanh pho nho vang nguoi hon la do thi lon
    @JsonProperty("city_pop")
    private Long cityPop;

    // Ngay sinh chu the (dinh dang "yyyy-MM-dd", vi du: "1985-03-22")
    // Dung de tinh tuoi -> feature "age" trong mo hinh ML
    private String dob;

    public PaymentTransaction() {} // Jackson can constructor rong de deserialize

    // ================================================================
    // GETTERS & SETTERS
    // Phan lon la getter/setter thong thuong.
    // Mot so co them @JsonProperty de xu ly truong hop ten truong
    // trong JSON khac voi ten bien trong Java.
    // ================================================================
    public Long getId()         { return id; }
    public void setId(Long id) { this.id = id; }

    // getAmt(): lay so tien giao dich (USD)
    public Double getAmt() { return amt != null ? amt : 0.0; }


    // Spring Boot gui "cc_num" (snake_case) -> @JsonProperty map vao bien ccNum (camelCase)
    public String getCcNum() { return ccNum; }
    public void setCcNum(String ccNum) { this.ccNum = ccNum; }

    // Xu ly truong hop JSON gui "cc_num" (snake_case tu Kafka)
    @JsonProperty("cc_num")
    public void setCc_num(String ccNum) { this.ccNum = ccNum; }

    public String getLocation()  { return location; }
    public void setLocation(String location) { this.location = location; }

    public void setStatus(String status) { this.    status = status; }

    public void setAmt(Double amt) { this.amt = amt; }

    public void setUnixTime(Long unixTime) { this.unixTime = unixTime; }

    // Xu ly ca truong hop JSON gui "unix_time" (snake_case)
    @JsonProperty("unix_time")
    public void setUnix_time(Long unixTime) { this.unixTime = unixTime; }

    public String getCategory()  { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getMerchant()  { return merchant; }
    public void setMerchant(String merchant) { this.merchant = merchant; }

    public Double getLat()       { return lat; }
    public void setLat(Double lat) { this.lat = lat; }

    public Double getLon()       { return lon; }
    public void setLon(Double lon) { this.lon = lon; }

    public Double getMerchLat()  { return merchLat; }
    public void setMerchLat(Double merchLat) { this.merchLat = merchLat; }

    // Xu ly ca truong hop JSON gui "merch_lat" (snake_case tu Kafka)
    @JsonProperty("merch_lat")
    public void setMerch_lat(Double v) { this.merchLat = v; }

    public Double getMerchLon()  { return merchLon; }
    public void setMerchLon(Double merchLon) { this.merchLon = merchLon; }

    @JsonProperty("merch_long")
    public void setMerch_long(Double v) { this.merchLon = v; }

    public Long getCityPop()     { return cityPop; }
    public void setCityPop(Long cityPop) { this.cityPop = cityPop; }

    @JsonProperty("city_pop")
    public void setCity_pop(Long v) { this.cityPop = v; }

    public String getDob()          { return dob; }
    public void setDob(String dob)   { this.dob = dob; }

    /**
     * calcAge(): tinh tuoi chu the tu ngay sinh.
     *
     * Moc tinh: 2020-01-01 (trung voi du lieu fraudTest.csv cua Kaggle)
     * Don vi: nam (so thap phan, vi du: 35.7 nam)
     *
     * Xu ly loi: neu dob null, rong, hoac sai dinh dang -> mac dinh 40 tuoi
     * (40 la gia tri trung binh gan dung, khong lam lech mo hinh qua nhieu)
     *
     * @return tuoi tinh bang nam, kieu double
     */
    public double calcAge() {
        if (dob == null || dob.isEmpty()) return 40.0;
        try {
            java.time.LocalDate birth = java.time.LocalDate.parse(dob); // parse "yyyy-MM-dd"
            java.time.LocalDate ref   = java.time.LocalDate.of(2020, 1, 1);
            return java.time.temporal.ChronoUnit.DAYS.between(birth, ref) / 365.0;
        } catch (Exception e) {
            return 40.0; // sai dinh dang ngay sinh -> dung gia tri mac dinh
        }
    }

    /**
     * toString(): in thong tin ngan gon de debug.
     * In them khoang cach (km) neu co du toa do.
     */
    @Override
    public String toString() {
        return String.format("Transaction{ccNum='%s', amount=%.2f, category='%s', merchant='%s', dist=%.1fkm}",
                getCcNum(), getAmt(), category, merchant,
                (lat != null && lon != null && merchLat != null && merchLon != null)
                        ? calcDistanceKm() : 0.0);
    }

    /**
     * calcDistanceKm(): tinh khoang cach nha -> merchant bang cong thuc Haversine.
     * Chi dung trong toString() de debug, khong dung trong logic chinh.
     * Logic chinh tinh distance trong callAIService() (FraudDetectionJob) va haversine() (main.py).
     *
     * Cong thuc Haversine tinh khoang cach chinh xac tren be mat cau cua Trai Dat.
     * Ban kinh Trai Dat R = 6371 km.
     */
    private double calcDistanceKm() {
        if (lat == null || lon == null || merchLat == null || merchLon == null) return 0;
        double R    = 6371;
        double phi1 = Math.toRadians(lat),      phi2 = Math.toRadians(merchLat);
        double dphi = Math.toRadians(merchLat - lat);
        double dlam = Math.toRadians(merchLon  - lon);
        double a    = Math.sin(dphi/2) * Math.sin(dphi/2)
                + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dlam/2) * Math.sin(dlam/2);
        return 2 * R * Math.asin(Math.sqrt(a));
    }
}