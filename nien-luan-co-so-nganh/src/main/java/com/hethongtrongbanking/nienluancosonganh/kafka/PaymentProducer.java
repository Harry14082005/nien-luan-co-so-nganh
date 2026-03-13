//Nguoi dua tin cho he thong
//Nhiem vu: Cam payment qua "buu dien Kafka" va gui vao dung TOPIC da dinh san
package com.hethongtrongbanking.nienluancosonganh.kafka;

import com.hethongtrongbanking.nienluancosonganh.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentProducer {

    @Autowired
    private KafkaTemplate<String, Payment> kafkaTemplate; // String -> Key, Payment -> Value

    private static final String TOPIC = "payment_transactions";

    public void sendPaymentEvent(Payment payment) {
        String ccNum = payment.getCcNum();

        // ✅ FIX PCI-DSS: Không bao giờ log số thẻ đầy đủ ra console hay file log.
        // Số thẻ đầy đủ trong log = vi phạm tiêu chuẩn PCI-DSS, có thể bị lộ nếu
        // log được lưu file, gửi lên monitoring system, hoặc ai đó xem terminal.
        // Chỉ hiện 4 số cuối để trace giao dịch mà không lộ thông tin nhạy cảm.
        String maskedCc = (ccNum != null && ccNum.length() >= 4)
                ? "****" + ccNum.substring(ccNum.length() - 4)
                : "****";

        log.info("📤 Gửi GD vào Kafka | CC={} | ${} | ID={}",
                maskedCc, payment.getAmt(), payment.getId());

        kafkaTemplate.send(TOPIC, ccNum, payment);
    }
}