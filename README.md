package com.citi.db.integration.memory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.citi.db.integration.core.AbstractBlockingIntegrationChannel;
import com.citi.db.integration.exception.IntegrationErrorCodes;
import com.citi.db.integration.exception.IntegrationException;

/**
 * @author dk99444
 *
 */
public class MemoryBlockingIntegrationChannel<I> extends AbstractBlockingIntegrationChannel<I> {
	//	private static final Logger _LOG = LoggerFactory.getLogger(MemoryIntegrationChannel.class);

	// The queue store
	private BlockingQueue<I> queue = null;

	public MemoryBlockingIntegrationChannel(String channelName, int capacity, I poisonPill) {
		super(channelName, capacity, poisonPill);

		queue = new ArrayBlockingQueue<I>(capacity, true);
	}

	@Override
	protected void doWrite(I item) throws IntegrationException {
		try {
			queue.put(item);
		} catch (InterruptedException e) {
			throw new IntegrationException(IntegrationErrorCodes.WRITE_FAILURE, e);
		}

	}

	@Override
	protected I doRead() throws IntegrationException {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			throw new IntegrationException(IntegrationErrorCodes.READ_FAILURE, e);
		}
	}

	@Override
	protected I doRead(long timeoutMillis) throws IntegrationException {
		try {
			return queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new IntegrationException(IntegrationErrorCodes.READ_FAILURE, e);
		}
	}

	@Override
	protected boolean isEmpty() throws IntegrationException {
		return queue.isEmpty();
	}

	@Override
	protected void doMarkForClosure() throws IntegrationException {
		//No-Op
	}

	@Override
	protected long getCurrentSize() throws IntegrationException {
		return queue.size();
	}

}
