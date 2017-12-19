package com.citi.db.integration.core;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.citi.db.integration.exception.IntegrationErrorCodes;
import com.citi.db.integration.exception.IntegrationException;

/**
 * @author dk99444
 *
 */
public class IntegrationChannelManager {
	private static final Logger _LOG = LoggerFactory.getLogger(IntegrationChannelManager.class);

	private static final IntegrationChannelManager SELF_INSTANCE;
	private static final Map<String, IntegrationChannel<?>> channelStore;
	private static final ThreadLocal<Map<String, IntegrationChannel<?>>> threadLocalChannels = new ThreadLocal<Map<String, IntegrationChannel<?>>>() {
		@Override
		protected Map<String, IntegrationChannel<?>> initialValue() {
			return new HashMap<String, IntegrationChannel<?>>();
		}
	};

	static {
		// Creating the static self-instance object
		SELF_INSTANCE = new IntegrationChannelManager();

		channelStore = new HashMap<String, IntegrationChannel<?>>();
	}

	/**
	 * Private constructor to make this a singleton class 
	 */
	private IntegrationChannelManager() {
		_LOG.debug("Creating IntegrationChannelManager instance.");
	}

	/**
	 * getInstance method for getting the singleton instance of IntegrationChannelManager
	 * @return
	 */
	public static IntegrationChannelManager getInstance() {
		_LOG.debug("Entering getInstance() method of IntegrationChannelManager.");
		return SELF_INSTANCE;
	}

	/**
	 * API to register a new channel with the integration channel manager.
	 * 
	 * @param channel
	 * @return
	 * @throws IntegrationException
	 */
	public IntegrationChannel<?> registerChannel(final IntegrationChannel<?> channel) throws IntegrationException {
		try {
			Assert.notNull(channel, "No channel sent for registering.");

			if (null != channelStore.get(channel.getName())) {
				throw new IntegrationException(IntegrationErrorCodes.CHANNEL_ALREADY_EXIST);
			}

			channel.setChannelCloseCallback(new IntegrationChannelCallback() {
				@Override
				public void afterClose() {
					removeChannel(channel.getName());
					try {
						_LOG.debug("channelStore after removing [{}]: {}", channel.getName(), channelStore);
					} catch (Exception e) {
						// Ignore error in case some exception thrown while logging. For e.g., ConcurrentModificationException on channelStore.
						_LOG.debug("channelStore after removing [{}], size: {}", channel.getName(), channelStore.size());
					}
				}
			});

			// Add to the channel store
			channelStore.put(channel.getName(), channel);
			threadLocalChannels.get().put(channel.getName(), channel);

			try {
				_LOG.debug("channelStore after registering [{}]: {}", channel.getName(), channelStore);
			} catch (Exception e) {
				// Ignore error in case some exception thrown while logging. For e.g., ConcurrentModificationException on channelStore.
				_LOG.debug("channelStore after registering [{}], size: {}", channel.getName(), channelStore.size());
			}

			return channel;
		} catch (IntegrationException e) {
			throw e;
		} catch (Exception e) {
			throw new IntegrationException(IntegrationErrorCodes.UNKNOWN_EXCEPTION, e);
		}
	}

	/**
	 * API to get an already registered channel from the integration channel manager.
	 * 
	 * @param channelName
	 * @return
	 * @throws IntegrationException
	 */
	public IntegrationChannel<?> getChannel(String channelName) throws IntegrationException {
		try {
			_LOG.debug("Requesting for channel: {}", channelName);
			// Find the channel
			final IntegrationChannel<?> channel = channelStore.get(channelName);
			_LOG.debug("Channel found for [{}]: {}", channelName, channel);

			if (null == channel) {
				throw new IntegrationException(IntegrationErrorCodes.CHANNEL_NOT_FOUND, "Channel'" + channelName + "'");
			}

			return channel;
		} catch (IntegrationException e) {
			throw e;
		} catch (Exception e) {
			throw new IntegrationException(IntegrationErrorCodes.UNKNOWN_EXCEPTION, e);
		}
	}

	/**
	 * API to clear channels created in the current thread
	 */
	public void clearThreadLocalChannels() {
		for (String channelName : threadLocalChannels.get().keySet()) {
			removeChannel(channelName);
		}
		threadLocalChannels.get().clear();
	}

	/**
	 * Remove the channel from store.
	 * 
	 * @param channelName
	 */
	void removeChannel(String channelName) {
		channelStore.remove(channelName);
	}
}
