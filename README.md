/**
 * 
 */
package com.citi.db.integration.core;

/**
 * @author dk99444
 *
 */
public interface IntegrationChannelCallback {
	/**
	 * Listen to the channel close event.
	 */
	void afterClose();
}
