package com.citi.db.integration.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * Exception class for Integration framework exceptions
 *
 */
public class IntegrationException extends NestableException {
	private static final long serialVersionUID = -8412921900443675338L;

	private IntegrationErrorCodes errCode = null;

	/**
	 * @return the errCode
	 */
	public IntegrationErrorCodes getVirtualizationErrorCode() {
		return errCode;
	}

	/**
	 * @param errCode
	 * @param errMessage
	 */
	public IntegrationException(IntegrationErrorCodes errCode) {
		super(errCode.getMessage());
		this.errCode = errCode;
	}

	/**
	 * @param errCode
	 * @param ex
	 */
	public IntegrationException(IntegrationErrorCodes errCode, Throwable ex) {
		super(errCode.getMessage(), ex);
		this.errCode = errCode;
	}

	/**
	 * @param errCode
	 * @param errMessage
	 */
	public IntegrationException(IntegrationErrorCodes errCode, String... errMessage) {
		super(errCode.getMessage(errMessage));
		this.errCode = errCode;
	}

	/**
	 * @param errCode
	 * @param errMessage
	 * @param ex
	 */
	public IntegrationException(IntegrationErrorCodes errCode, String errMessage, Throwable ex) {
		super(errCode.getMessage() + " " + errMessage, ex);
		this.errCode = errCode;
	}

	public boolean hasErrorCode(final IntegrationErrorCodes errCode) {
		// Check if the Error Code is present in this instance of Error
		if (errCode.equals(this.errCode)) {
			return true;
		}

		// Check if the Error Code is present in parent instance of Error
		Throwable parentEx = getCause();
		if (parentEx instanceof IntegrationException) {
			return ((IntegrationException) parentEx).hasErrorCode(errCode);
		}

		return false;
	}
}
