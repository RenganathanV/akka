/*
 * 
 */
package com.sample.assignment.actor;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;

public class Aggregator extends AbstractLoggingActor {

	private Map<Path, Integer> fileWordCount;

	public Aggregator() {
		fileWordCount = new ConcurrentHashMap<Path, Integer>();
	}

	public static class Status {
	}

	/** Generic message for aggregator.
	 * The Class AggregatorMessage.
	 */
	static class AggregatorMessage {
		private Path filePath;

		public AggregatorMessage(Path fPath) {
			this.filePath = fPath;
		}

		public Path getFilePath() {
			return filePath;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((filePath == null) ? 0 : filePath.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AggregatorMessage other = (AggregatorMessage) obj;
			if (filePath == null) {
				if (other.filePath != null)
					return false;
			} else if (!filePath.equals(other.filePath))
				return false;
			return true;
		}

	}

	public static class SOF extends AggregatorMessage {

		public SOF(Path path) {
			super(path);
		}

	}

	public static class EOF extends AggregatorMessage {

		public EOF(Path path) {
			super(path);
		}
	}

	public static class Line extends AggregatorMessage {
		String line;

		public Line(Path path, String line) {
			super(path);
			this.line = line;
		}

		public String getLine() {
			return line;
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(SOF.class, sof -> {
					handleSOFMessage(sof);
				})
				.match(Line.class, line -> {
					handleLineMessage(line);
				})
				.match(EOF.class, eof -> {
					handleEOFMessage(eof);
				})
				.match(Status.class, status -> {
					handleStatusMsg();
				})
				.matchAny(
						o -> {
							log().warning(
									String.format("Received unhandled message to Aggregator"));
						}).build();
	}

	private void handleStatusMsg() {
		getSender().tell(fileWordCount.size(), getSelf());

	}

	/**
	 * Handle End of file message.
	 * Log the number of words in the file.
	 * @param eof the eof
	 */
	private void handleEOFMessage(EOF eof) {
		if (fileWordCount.containsKey(eof.getFilePath())) {
			log().info(
					String.format("Total number of words in the file %s : %d.",
							eof.getFilePath(),
							fileWordCount.get(eof.getFilePath())));
			getSender().tell(fileWordCount.get(eof.getFilePath()), self());
		} else {
			log().info(String.format("Unknown path received with EOF message"));
		}
	}

	/**
	 * Count the number of words in line and update the fileWordCount variable.
	 * @param line
	 *            the line
	 */
	private void handleLineMessage(Line line) {
		if (fileWordCount.containsKey(line.getFilePath())) {
			fileWordCount.put(line.getFilePath(), Integer.sum(fileWordCount
					.get(line.getFilePath()),
					line.getLine().split("\\s").length));

		} else {
			log().info(String.format("Unknown path received with EOF message"));
		}

	}

	/**
	 * Insert the new file path in to map.
	 * @param sof
	 *            the sof
	 */
	private void handleSOFMessage(SOF sof) {
		log().info(
				String.format(
						"Received start of file message from file parser for the file %s",
						sof.getFilePath()));
		fileWordCount.put(sof.getFilePath(), 0);
	}

	public static Props props() {
		return Props.create(Aggregator.class);
	}

}
