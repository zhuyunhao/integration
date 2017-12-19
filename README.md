package com.citi.db.integration.memory.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.citi.db.integration.core.IntegrationChannel;
import com.citi.db.integration.core.IntegrationChannelManager;
import com.citi.db.integration.exception.IntegrationException;
import com.citi.db.integration.memory.MemoryBlockingIntegrationChannel;

@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = { "/com/citi/ual/framework/conf/virtualization-spring-config.xml" })
public class TestMemoryBlockingIntergationChannel {

	@Test
	public void testChannel() throws IntegrationException, InterruptedException, ExecutionException {

		final String channelName = "Queue#1";
		final String poisonPill = "STOP";

		IntegrationChannelManager.getInstance().registerChannel(new MemoryBlockingIntegrationChannel<String>(channelName, 2, poisonPill));

		final List<Callable<String>> workerTasks = new ArrayList<>();
		final List<StringBuffer> resultTexts = new ArrayList<>();

		// Producer Thread
		final StringBuffer resBuffProducer = new StringBuffer();
		resultTexts.add(resBuffProducer);
		workerTasks.add(new Callable<String>() {
			@SuppressWarnings("unchecked")
			@Override
			public String call() {
				try {
					List<String> itemList = new ArrayList<>();
					itemList.add("First Item");
					itemList.add("Second Item");
					itemList.add("Third Item");
					itemList.add("Fourth Item");
					itemList.add("Fifth Item");

					Thread.sleep(2000);

					IntegrationChannel<String> channel = (IntegrationChannel<String>) IntegrationChannelManager.getInstance().getChannel(channelName);
					int idx = 0;
					while (idx < itemList.size()) {
						System.out.println("Before Send on '" + channel.getName() + "' item: " + itemList.get(idx));
						channel.write(itemList.get(idx));
						System.out.println("After Send on '" + channel.getName() + "' item: " + itemList.get(idx));
						idx++;

						// Simulating processing delay
						// Thread.sleep(100);

						// Simulating failure
						//						if (idx == 4) {
						//							throw new InterruptedException("Simulated producer failure.");
						//						}
					}
					channel.markForClosure();
					resBuffProducer.append("SUCCESS: producer done.");
					return "SUCCESS: producer done.";
				} catch (Exception e) {
					resBuffProducer.append("ERROR: (Producer) :" + e.getMessage());
					System.out.println("resBuffProducer:" + resBuffProducer);
					System.err.println(e.getMessage());
					//e.printStackTrace();
					return resBuffProducer.toString();
				}
			}
		});

		// Consumer Thread
		final StringBuffer resBuffConsumer = new StringBuffer();
		resultTexts.add(resBuffConsumer);
		workerTasks.add(new Callable<String>() {
			@SuppressWarnings("unchecked")
			@Override
			public String call() throws Exception {
				try {
					IntegrationChannel<String> channel = (IntegrationChannel<String>) IntegrationChannelManager.getInstance().getChannel(channelName);
					boolean channelOpen = true;
					int idx = 0;
					while (channelOpen) {
						final String item = channel.read();
						System.out.println("Received on '" + channel.getName() + "': " + item);

						if (poisonPill.equalsIgnoreCase(item)) {
							channelOpen = false;
						}

						idx++;

						// Simulating processing delay
						//Thread.sleep(1000);

						// Simulating failure
//						if (idx == 1) {
//							throw new InterruptedException("Simulated consumer failure.");
//						}
					}
					System.out.println("Channel closed : " + channel.isClosed());
					resBuffConsumer.append("SUCCESS: consumer done.");
					return "SUCCESS: consumer done.";
				} catch (Exception e) {
					resBuffConsumer.append("ERROR: (Consumer) :" + e.getMessage());
					System.out.println("resBuffConsumer:" + resBuffConsumer);
					System.err.println(e.getMessage());
					//e.printStackTrace();
					return resBuffConsumer.toString();
				}

			}
		});

		// Run all the steps asynchronously
		final ExecutorService executor = Executors.newFixedThreadPool(5);
		final CompletionService<String> completionService = new ExecutorCompletionService<String>(executor);

		//		final List<Future<String>> results = executor.invokeAll(workerTasks);
		final List<Future<String>> results = new ArrayList<Future<String>>(); //executor.invokeAll(workerTasks);
		for (int i = 0; i < workerTasks.size(); i++) {
			results.add(completionService.submit(workerTasks.get(i)));
		}

		for (int i = 0; i < results.size(); i++) {
			try {
				System.out.println("wait for result**:");
				Future<String> result = completionService.take();
				System.out.println("result**:" + result);
				System.out.println("result****:" + result.get());

				if (result.get().startsWith("ERROR")) {
					System.out.println("Cancelling all tasks.");
					executor.shutdownNow();
					//					for (Future<String> future : results) {
					//						if (!future.isDone()) {
					//							System.out.println("Cancelling " + future);
					//							future.cancel(true);
					//						}
					//					}
				}
			} catch (InterruptedException e) {
				System.out.println("InterruptedException ::" + e);
			} catch (ExecutionException e) {
				System.out.println("ExecutionException ::" + e);

			}
		}

		//final result
		System.out.println("** FINAL RESULT **");
		for (StringBuffer sb : resultTexts) {
			System.out.println("Final ***** result: " + sb.toString());
		}

		System.out.println("Executor Shutdown.");
		executor.shutdown();
	}
}
