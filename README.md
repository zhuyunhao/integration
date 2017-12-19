package com.citi.db.integration.exception;

public enum IntegrationErrorCodes {

	//GENERAL
	NO_SPACE("No Space Available in the Channel."),//
	READ_TIMEOUT("Timedout waiting for data. Timeout millis: '#1#'"),//
	
	// Channel Factory Error Codes
	CHANNEL_ALREADY_EXIST("Requested channel already exist, can not create new."),

	CHANNEL_NOT_FOUND("Requested Channel not found."),

	// Channel Implementation error codes
	READ_FAILURE("Unable to read from the channel."),

	WRITE_FAILURE("Unable to write to the channel."),

	UNKNOWN_EXCEPTION("Unknown System Error");

	private String message;

	IntegrationErrorCodes(String message) {
		this.message = message;
	}

	public String getMessage(String... params) {
		int cnt = 1;
		String msg = this.message;
		if (params != null && params.length > 0) {
			for (String par : params) {
				msg = msg.replaceAll("#" + cnt + "#", par);
				cnt++;
			}
		}
		this.message = msg;
		return msg;
	}

	public String getCode() {
		return name();
	}

}
