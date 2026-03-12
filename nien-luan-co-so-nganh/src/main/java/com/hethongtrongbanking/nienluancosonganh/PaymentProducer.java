//Nguoi dua tin cho he thong
//Nhiem vu: Cam payment qua "buu dien Kafka" va gui vao dung TOPIC da dinh san
package com.hethongtrongbanking.nienluancosonganh;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Locale;

@Service
//Linh kien (Bean). Nho vay co the Autowired o batky dau ma khong can new
@Slf4j
//Tien ich thu vien Lombok: khong phai ghi dai dong (private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);)
//Vi neu muon dung Log thi phai khai bao o tung class -> de sai
public class PaymentProducer {
    @Autowired
    private KafkaTemplate<String, Payment> kafkaTemplate;//IMPORTANT, String -> Key, Payment -> Value
    //An di toan bo cac buoc ket noi phuc tap ben duoi, gui tnhan trong 1 lenh

    private static  final String TOPIC = "payment_transactions";
    //Ten cua channel muon phat song, ai muon giao dich phai nghe dung kenh nay
    public void sendPaymentEvent (Payment payment){
        log.info("Sending payment to Kafka: {}", payment.getCcNum());
        //In thong tin ra man Console, nho co thu vien Lombok ma code ngan
        kafkaTemplate.send(TOPIC, payment.getCcNum(), payment);
        // TOPIC: gui vao dau?, payment.getCardID(): gan nhan gi?, payment: gui ndung gi?

    }

}
