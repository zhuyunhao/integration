package com.citi.db.integration.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.citi.db.integration.exception.IntegrationErrorCodes;
import com.citi.db.integration.exception.IntegrationException;

/**
 * @author dk99444
 *
 */
public abstract class AbstractBlockingIntegrationChannel<I> implements IntegrationChannel<I> {
	private static final Logger _LOG = LoggerFactory.getLogger(AbstractBlockingIntegrationChannel.class);

	private IntegrationChannelCallback managerCallback = null;
	private String channelName = null;
	private long capacity = 0;
	private I poisonPill = null;

	private boolean markForClosure = false;
	private boolean closed = false;

	/**
	 * @param channelName, the name of the channel
	 * @param capacity, the capacity of the channel
	 * @param poisonPill, the Poison Pill. 
	 * 					  This is the last message posted in the queue indicating the receiver that no more data is expected.
	 * 					  If the termination signal is ignored, the channel will be closed and will throw an error on next read.
	 */
	public AbstractBlockingIntegrationChannel(String channelName, long capacity, I poisonPill) {
		Assert.notNull(channelName, "Channel name can not be null.");
		Assert.notNull(poisonPill, "Poison Pill can not be null.");

		this.channelName = channelName;
		this.capacity = capacity;
		this.poisonPill = poisonPill;
	}

	@Override
	public String getName() {
		return channelName;
	}

	@Override
	public void setChannelCloseCallback(IntegrationChannelCallback managerCallback) {
		this.managerCallback = managerCallback;
	}

	@Override
	public void write(I item) throws IntegrationException {
		if (markForClosure) {
			throw new IntegrationException(IntegrationErrorCodes.WRITE_FAILURE, "The channel is marked for closure.");
		}
		if (closed) {
			throw new IntegrationException(IntegrationErrorCodes.WRITE_FAILURE, "The channel is closed.");
		}
		doWrite(item);
	}

	@Override
	public I read() throws IntegrationException {
		if (closed && isEmpty()) {
			throw new IntegrationException(IntegrationErrorCodes.READ_FAILURE, "The channel is closed with no more data to read. this may happen if consumer has not handled the Termination Message.");
		}

		I item = doRead();
		if (markForClosure && isEmpty()) {
			closeChannel();
		}
		return item;
	}

	@Override
	public I read(long timeoutMillis) throws IntegrationException {
		if (closed && isEmpty()) {
			throw new IntegrationException(IntegrationErrorCodes.READ_FAILURE, "The channel is closed with no more data to read. this may happen if consumer has not handled the Termination Message.");
		}

		I item = doRead(timeoutMillis);
		
		if(null== item) {
			throw new IntegrationException(IntegrationErrorCodes.READ_TIMEOUT, timeoutMillis+"");
		}
		
		if (markForClosure && isEmpty()) {
			closeChannel();
		}
		return item;
	}
	
	@Override
	public void markForClosure() throws IntegrationException {
		_LOG.debug("Marking channel '{}' for closure.", channelName);
		if (markForClosure) {
			return;
		}

		markForClosure = true;

		// Put the poison-pill in the channel indicating the end for data stream.
		doWrite(poisonPill);
		_LOG.debug("Poison-Pill sent on channel '{}'.", channelName);

		doMarkForClosure();
	}

	@Override
	public boolean isClosed() throws IntegrationException {
		return closed;
	}

	@Override
	public boolean isFull() throws IntegrationException {
		return (getCurrentSize() >= capacity);
	}

	/**
	 * @param item
	 * @throws IntegrationException
	 */
	abstract protected void doWrite(I item) throws IntegrationException;

	/**
	 * @return
	 * @throws IntegrationException
	 */
	abstract protected I doRead() throws IntegrationException;
	
	/**
	 * @return
	 * @throws IntegrationException
	 */
	abstract protected I doRead(long timeoutMillis) throws IntegrationException;

	/**
	 * @throws IntegrationException
	 */
	abstract protected void doMarkForClosure() throws IntegrationException;

	/**
	 * @return
	 * @throws IntegrationException
	 */
	abstract protected boolean isEmpty() throws IntegrationException;

	/**
	 * @return
	 * @throws IntegrationException
	 */
	abstract protected long getCurrentSize() throws IntegrationException;

	/**
	 * @throws IntegrationException
	 */
	private void closeChannel() throws IntegrationException {
		if (closed) {
			return;
		}

		_LOG.debug("Closing channel '{}'.", channelName);

		closed = true;

		if (null != managerCallback) {
			managerCallback.afterClose();
		}
	}

}
