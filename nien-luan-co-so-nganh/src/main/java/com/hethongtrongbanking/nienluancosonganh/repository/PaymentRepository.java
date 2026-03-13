package com.hethongtrongbanking.nienluancosonganh.repository;

import com.hethongtrongbanking.nienluancosonganh.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {
    // Payment: class gan @Entity, Long: datatype of PK
    // Mỗi object Payment = 1 dòng trong bảng payment
    // Không cần viết gì thêm, JpaRepository đã lo sẵn các hàm lưu, xóa, tìm kiếm.

}
