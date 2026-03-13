package com.hethongtrongbanking.nienluancosonganh.exception;

import com.hethongtrongbanking.nienluancosonganh.model.FraudCaseStatus;

/**
 * ✅ FIX: Custom exception thay thế RuntimeException("đã được xử lý...").
 *
 * Dùng khi: analyst cố xử lý một FraudCase đã có status != OPEN.
 * HTTP status: 409 Conflict
 */
public class AlreadyResolvedException extends RuntimeException {

    public AlreadyResolvedException(Long caseId, FraudCaseStatus currentStatus) {
        super("FraudCase ID=" + caseId + " đã được xử lý (status=" + currentStatus + ")");
    }
}