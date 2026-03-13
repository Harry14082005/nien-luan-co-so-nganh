package com.hethongtrongbanking.nienluancosonganh.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ================================================================
 * GLOBAL EXCEPTION HANDLER
 * ================================================================
 *
 * ✅ FIX: Bắt theo exception TYPE thay vì đọc message string.
 *
 * Vấn đề cũ:
 *   if (message.contains("Không tìm thấy")) → 404   ← giòn, dễ vỡ
 *   if (message.contains("đã được xử lý"))  → 409   ← phụ thuộc wording
 *
 * Giải pháp mới — mỗi loại lỗi có exception riêng:
 *   ResourceNotFoundException  → 404 Not Found
 *   AlreadyResolvedException   → 409 Conflict
 *   BusinessException          → 400 Bad Request
 *   MethodArgumentNotValidException → 400 (validation @Valid)
 *   Exception                  → 500 Internal Server Error
 *
 * Lợi ích:
 *   - Type-safe: đổi message không ảnh hưởng HTTP status
 *   - Dễ test: mock đúng exception type là đủ
 *   - Dễ mở rộng: thêm exception mới chỉ cần thêm @ExceptionHandler
 * ================================================================
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * 404 Not Found — entity không tồn tại trong DB.
     * Ném từ: PaymentService, FraudCaseService khi findById() không có kết quả.
     */
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(ResourceNotFoundException ex) {
        log.warn("⚠️  Not Found: {}", ex.getMessage());
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(errorBody(HttpStatus.NOT_FOUND, ex.getMessage()));
    }

    /**
     * 409 Conflict — analyst cố xử lý FraudCase đã resolved.
     * Ném từ: FraudCaseService.getOpenCase() khi status != OPEN.
     */
    @ExceptionHandler(AlreadyResolvedException.class)
    public ResponseEntity<Map<String, Object>> handleConflict(AlreadyResolvedException ex) {
        log.warn("⚠️  Conflict: {}", ex.getMessage());
        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(errorBody(HttpStatus.CONFLICT, ex.getMessage()));
    }

    /**
     * 400 Bad Request — vi phạm rule nghiệp vụ chung.
     * Ném từ: bất kỳ Service nào khi input không hợp lệ về mặt nghiệp vụ.
     */
    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<Map<String, Object>> handleBusiness(BusinessException ex) {
        log.warn("⚠️  Business Error: {}", ex.getMessage());
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorBody(HttpStatus.BAD_REQUEST, ex.getMessage()));
    }

    /**
     * 400 Bad Request — @Valid thất bại (field bị null, blank, sai format...).
     * Tự động được ném khi dùng @Valid trên @RequestBody trong Controller.
     * Gom tất cả lỗi validation thành 1 message rõ ràng.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        String errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .collect(Collectors.joining(", "));
        log.warn("⚠️  Validation thất bại: {}", errors);
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorBody(HttpStatus.BAD_REQUEST, "Dữ liệu không hợp lệ: " + errors));
    }

    /**
     * 500 Internal Server Error — lỗi không mong đợi (DB down, NullPointer...).
     * Luôn log đầy đủ stack trace để debug.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception ex) {
        log.error("❌ Lỗi hệ thống không mong đợi: {}", ex.getMessage(), ex);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorBody(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Lỗi hệ thống, vui lòng thử lại sau"));
    }

    // ── Helper: tạo response body chuẩn ─────────────────────────
    private Map<String, Object> errorBody(HttpStatus status, String message) {
        return Map.of(
                "timestamp", LocalDateTime.now().toString(),
                "status",    status.value(),
                "error",     status.getReasonPhrase(),
                "message",   message
        );
    }
}