package com.hethongtrongbanking.nienluancosonganh.exception;

/**
 * ✅ FIX: Custom exception cho lỗi nghiệp vụ chung.
 *
 * Dùng khi: vi phạm rule nghiệp vụ không thuộc 404/409.
 * HTTP status: 400 Bad Request
 */
public class BusinessException extends RuntimeException {

    public BusinessException(String message) {
        super(message);
    }
}