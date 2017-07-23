package com.sample.assignment.actor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class FileScanner extends AbstractLoggingActor {

	private ActorRef fileParserActor;
	private Integer fileCount = 0;
	private Boolean completed = false;

	public FileScanner(ActorRef actorRef) {
		this.fileParserActor = actorRef;
	}

	public static class Scan {
		private final Path directoryLocation;

		public Scan(Path directoryLocation) {
			this.directoryLocation = directoryLocation;
		}

		public Path getDirectoryLocation() {
			return directoryLocation;
		}
	}

	public static class Status {
	}

	public static class FileCount {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Scan.class, scanMessage -> {
					scan(scanMessage);
				})
				.match(FileCount.class, fileCountMessage -> {
					getSender().tell(fileCount, self());
				})
				.match(Status.class, statusMessage -> {
					getSender().tell(completed, self());
				})
				.matchAny(
						o -> {
							log().info(
									String.format("Received un handled message in scanner."));
							getSender().tell("Unhandled Message Type", self());
						}).build();

	}

	/**
	 * Scan : Check the file path and create a parser and aggregator actor for
	 * valid files and pass the parse message to the parser actor.
	 *
	 * @param scanMessage
	 *            the scan message
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void scan(Scan scanMessage) throws IOException {
		Files.walk(scanMessage.getDirectoryLocation()).forEach(
				path -> {
					if (Files.isRegularFile(path)) {
						log().info(
								String.format(
										"Invoking file parser for the file %s",
										path));

						fileParserActor.tell(new FileParser.Parse(path),
								getSender());
						fileCount++;

					} else {
						log().info(
								String.format("Not valid file. Skipping - %s",
										path));
					}
				});
		completed = true;
	}

	public static Props props(ActorRef actorRef) {
		return Props.create(FileScanner.class, actorRef);
	}
}
