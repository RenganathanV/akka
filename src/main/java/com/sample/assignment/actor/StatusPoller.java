/*
 * 
 */
package com.sample.assignment.actor;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Poller to check the completion of processing and terminate the system.
 */
public class StatusPoller extends AbstractLoggingActor {

	private ThreadMXBean mxBean;
	private ActorRef fileAggregator = null;
	private ActorRef fileScanner = null;
	private Integer fileScannerProcessedCount = 0;
	private Boolean fileScannerCompleted = false;

	public StatusPoller(ActorRef aggRef, ActorRef parserRef) {
		this.fileAggregator = aggRef;
		this.fileScanner = parserRef;
		mxBean = ManagementFactory.getThreadMXBean();
	}

	public void setFileScannerProcessedCount(Integer fileScannerProcessedCount) {
		this.fileScannerProcessedCount = fileScannerProcessedCount;
	}

	public void setFileScannerCompleted(Boolean fileScannerCompleted) {
		this.fileScannerCompleted = fileScannerCompleted;
	}

	public static class Check {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Check.class, check -> {
					handleCheckMessage();
				})
				.match(Boolean.class, filesProcessing -> {
					handleStatusResponse(filesProcessing, getSender());
				})
				.match(Integer.class, fileCount -> {
					handleActorFileCount(fileCount, getSender());
				})
				.matchAny(
						o -> {
							log().info(
									String.format("Received un handled message in performance actor."));
						}).build();
	}

	/**
	 * Checks the number of files processed by file scanner and aggregator. If
	 * all the files are processed in the folder, terminate the system.
	 * 
	 * @param fileCount
	 *            the file count
	 * @param sender
	 *            the sender
	 */
	private void handleActorFileCount(Integer fileCount, ActorRef sender) {
		if (sender.equals(fileScanner)) {
			log().info(
					String.format(
							"Total number of files processed in scanner - %d ",
							fileCount));
			fileScannerProcessedCount = fileCount;
		} else if (sender.equals(fileAggregator)) {
			if (fileCount.equals(fileScannerProcessedCount)) {
				log().info(
						String.format("Aggregator completed processing files. Terminating the system."));
				getContext().system().terminate();
			} else {
				log().info(
						String.format(
								"Number of files processed in aggregator - %d ",
								fileCount));
			}
		}

	}

	/**
	 * Check the file scanner actor for completion. If file scanner has completed
	 * processing, send message to file scanner to get number
	 * of files processed.
	 * 
	 * @param filesProcessing
	 *            the files processing
	 */
	private void handleStatusResponse(Boolean filesProcessing, ActorRef actorRef) {
		if (filesProcessing && actorRef.equals(fileScanner)) {
			System.out.println("File Scanner completed processing files.");
			fileScannerCompleted = true;
			actorRef.tell(new FileScanner.FileCount(), self());
		}

	}

	/**
	 * 1) Logs the number of active thread in JVM.
	  2) Poll for the completion of file scanner. 3) If file scanner has
	 * completed processing all the files, Poll aggregator for completion.
	 */
	private void handleCheckMessage() {
		log().info(
				String.format(
						"Number of active thread (including daemon) : %d",
						mxBean.getThreadCount()));
		log().info(
				String.format("Peak live thread count : %d",
						mxBean.getPeakThreadCount()));

		log().info(
				String.format("Daemon thread count : %d",
						mxBean.getDaemonThreadCount()));

		if (!fileScannerCompleted) {
			fileScanner.tell(new FileScanner.Status(), self());
		} else {
			fileAggregator.tell(new Aggregator.Status(), getSelf());
		}

	}

	public static Props props(ActorRef aggRef, ActorRef scannerRef) {
		return Props.create(StatusPoller.class, aggRef, scannerRef);
	}

}
