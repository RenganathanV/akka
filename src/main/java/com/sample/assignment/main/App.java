package com.sample.assignment.main;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.sample.assignment.actor.Aggregator;
import com.sample.assignment.actor.FileParser;
import com.sample.assignment.actor.FileScanner;
import com.sample.assignment.actor.StatusPoller;

public class App {

	//Directory hard coded. Modify to test.
	private static Path myDir = Paths.get("F:/Data");
	private static ActorSystem system;

	/**
	 * Entry point for the application.
	 * Creates actors and invoke the process for hard coded directory.
	 * Invoke scheduler to check for the completion of processing.
	 * @param args the arguments
	 */
	public static void main(String args[]) {
		system = ActorSystem.create("Log-Processor");
		ActorRef aggregatorRef = getActorReference(Aggregator.props(),
				"aggregator");
		ActorRef fileParrser = getActorReference(
				FileParser.props(aggregatorRef), "parser");
		ActorRef fileScanner = getActorReference(
				FileScanner.props(fileParrser), "scanner");
		ActorRef statusChecker = getActorReference(
				StatusPoller.props(aggregatorRef, fileScanner),
				"statusChecker");
		//Invoke the master
		fileScanner.tell(new FileScanner.Scan(myDir), ActorRef.noSender());
		checkSystemForTermination(statusChecker);
	}

	/**
	 * Check system for termination.
	 * Scheduler to check JVM threads and terminate system after file processing.
	 * @param statusChecker the status checker
	 */
	private static void checkSystemForTermination(ActorRef statusChecker) {
		system.scheduler().schedule(
				Duration.create(1000, TimeUnit.MILLISECONDS),
				Duration.create(2000, TimeUnit.MILLISECONDS), statusChecker,
				new StatusPoller.Check(), system.dispatcher(), null);

	}

	private static ActorRef getActorReference(Props props, String actorName) {
		return system.actorOf(props, actorName);
	}
}
