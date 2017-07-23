/*
 * 
 */
package com.sample.assignment.actor;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import com.sample.assignment.actor.Aggregator.Line;
import com.sample.assignment.actor.Aggregator.SOF;

public class FileParser extends AbstractLoggingActor {

	private ActorRef fileAggregator = null;

	public FileParser(ActorRef actorRef) {
		this.fileAggregator = actorRef;
	}

	public static class Parse {
		private final Path fileLocation;

		public Parse(Path fileLocation) {
			this.fileLocation = fileLocation;
		}

		public Path getFileLocation() {
			return fileLocation;
		}

	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Parse.class, parseMessage -> {
					handleParseMessage(parseMessage);
				})
				.matchAny(
						o -> {
							log().warning(
									String.format("Received unhandled message to Parser"));
							getSender().tell("Unhandled Message Type", self());
						}).build();

	}

	/**
	 * Process single file and invoke aggregator for start of file, 
	 * line and end of file events.
	 *
	 * @param parseMessage the parse message
	 */
	private void handleParseMessage(Parse parseMessage)throws IOException {
		Path filePath = parseMessage.getFileLocation();
		try {
			BufferedReader reader = Files.newBufferedReader(filePath,
					StandardCharsets.UTF_8);
			fileAggregator.tell(new SOF(filePath), getSender());

			String line = null;
			while ((line = reader.readLine()) != null) {
				fileAggregator.tell(new Line(filePath, line), getSelf());
			}
			reader.close();
			fileAggregator.tell(new Aggregator.EOF(filePath), getSelf());
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static Props props(ActorRef actorRef) {
		return Props.create(FileParser.class, actorRef);
	}

}
