package com.sample.assignment.actor;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;

import com.sample.assignment.actor.Aggregator;
import com.sample.assignment.actor.FileScanner;
import com.sample.assignment.actor.StatusPoller;

public class TestStatusPoller {
	static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create();
	}

	@AfterClass
	public static void teardown() {
		TestKit.shutdownActorSystem(system,
				Duration.create(10000, TimeUnit.MILLISECONDS), false);
		system = null;
	}

	@Test
	public void testScannerStatusHandler() {
		TestProbe scanner = new TestProbe(system);
		TestProbe aggregator = new TestProbe(system);
		final ActorRef statusChecker = system.actorOf(
				StatusPoller.props(aggregator.ref(), scanner.ref()),
				"status-checker-1");
		statusChecker.tell(new StatusPoller.Check(), ActorRef.noSender());
		// check scanner
		scanner.expectMsgClass(FileScanner.Status.class);
	}
	
	@Test
	public void testFileScannerFileCountHandler(){
		TestProbe scanner = new TestProbe(system);
		TestProbe aggregator = new TestProbe(system);
		final ActorRef statusChecker = system.actorOf(
				StatusPoller.props(aggregator.ref(), scanner.ref()),
				"status-checker-2");
		statusChecker.tell(new Boolean(true), scanner.ref());
		scanner.expectMsgClass(FileScanner.FileCount.class);
		
	}

	@Test
	public void testAggregatorDependency() {
		TestProbe parser = new TestProbe(system);
		TestProbe aggregator = new TestProbe(system);
		final ActorRef fileScanner = system.actorOf(
				FileScanner.props(parser.ref()), "scanner-2");
		final ActorRef statusChecker = system.actorOf(
				StatusPoller.props(aggregator.ref(), fileScanner),
				"status-checker-3");
		statusChecker.tell(new StatusPoller.Check(), ActorRef.noSender());
		statusChecker.tell(new Boolean(true), fileScanner);
		statusChecker.tell(new StatusPoller.Check(), ActorRef.noSender());
		// check aggregator
		aggregator.expectMsgClass(Aggregator.Status.class);
	}

	@Test
	public void testSystemTermination() throws Exception {
		TestProbe parser = new TestProbe(system);
		TestProbe aggregator = new TestProbe(system);
		final ActorRef fileScanner = system.actorOf(
				FileScanner.props(parser.ref()), "scanner-3");
		final ActorRef statusChecker = system.actorOf(
				StatusPoller.props(aggregator.ref(), fileScanner),
				"status-checker-4");
		// send file scanner completed message
		statusChecker.tell(new Boolean(true), fileScanner);
		statusChecker.tell(1, fileScanner);

		// send aggregator completed message
		statusChecker.tell(1, aggregator.ref());
		Await.result(system.whenTerminated(),
				Duration.create(250, TimeUnit.MILLISECONDS));
	}

}
