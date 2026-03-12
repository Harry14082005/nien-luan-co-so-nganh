package com.hethongtrongbanking.nienluancosonganh;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * ================================================================
 * GLOBAL EXCEPTION HANDLER
 * ================================================================
 *
 * Vấn đề cần giải quyết:
 *   RuntimeException("Không tìm thấy ID=99")
 *   → Spring mặc định trả 500 Internal Server Error
 *   → Client không biết lỗi do gì
 *
 * Giải pháp:
 *   @RestControllerAdvice bắt exception từ tất cả Controller,
 *   map sang đúng HTTP status code + message rõ ràng.
 *
 * Mapping:
 *   "Không tìm thấy..."  → 404 Not Found
 *   "đã được xử lý..."  → 409 Conflict   (case đã resolve rồi)
 *   RuntimeException     → 400 Bad Request (lỗi nghiệp vụ chung)
 *   Exception            → 500 Internal Server Error (lỗi không mong đợi)
 * ================================================================
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * Bắt RuntimeException — lỗi nghiệp vụ từ Service layer.
     *
     * Phân loại theo message để trả đúng HTTP code:
     *   "Không tìm thấy" → 404
     *   "đã được xử lý"  → 409
     *   còn lại          → 400
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleRuntimeException(RuntimeException ex) {
        String message = ex.getMessage() != null ? ex.getMessage() : "Lỗi không xác định";

        HttpStatus status;
        if (message.contains("Không tìm thấy")) {
            status = HttpStatus.NOT_FOUND;                  // 404
            log.warn("⚠️  Not Found: {}", message);
        } else if (message.contains("đã được xử lý")) {
            status = HttpStatus.CONFLICT;                   // 409
            log.warn("⚠️  Conflict: {}", message);
        } else {
            status = HttpStatus.BAD_REQUEST;                // 400
            log.warn("⚠️  Bad Request: {}", message);
        }

        return ResponseEntity.status(status).body(errorBody(status, message));
    }

    /**
     * Bắt Exception chung — lỗi không mong đợi (NullPointer, DB down...).
     * Luôn trả 500, log đầy đủ stack trace để debug.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception ex) {
        log.error("❌ Lỗi hệ thống không mong đợi: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
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