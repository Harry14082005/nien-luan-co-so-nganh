package com.hethongtrongbanking.nienluancosonganh.exception;

/**
 * ✅ FIX: Custom exception thay thế RuntimeException("Không tìm thấy...").
 *
 * Vấn đề cũ:
 *   GlobalExceptionHandler phải đọc message string để quyết định HTTP status:
 *   if (message.contains("Không tìm thấy")) → 404
 *   → Giòn: đổi wording là vỡ, không type-safe, khó test.
 *
 * Giải pháp:
 *   Ném đúng exception type → Handler bắt đúng type → map đúng HTTP status.
 *   Không phụ thuộc vào nội dung message nữa.
 *
 * Dùng khi: không tìm thấy entity theo ID trong DB.
 * HTTP status: 404 Not Found
 */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }

    // Overload tiện dụng: ResourceNotFoundException("Payment", 99L)
    // → tự sinh message: "Không tìm thấy Payment với ID=99"
    public ResourceNotFoundException(String entityName, Long id) {
        super("Không tìm thấy " + entityName + " với ID=" + id);
    }
}