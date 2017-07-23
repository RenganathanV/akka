package com.sample.assignment.actor;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sample.assignment.actor.Aggregator;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;

public class TestAggregator {
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
	public void testAggregatorOperations() {
		File file = new File("src/test/resources/home.txt");
		String absolutePath = file.getAbsolutePath();
		Path filePath = Paths.get(absolutePath);
		String lineContent = "home home home home";
		TestProbe parent = new TestProbe(system);
		final ActorRef aggregatorTestRef = system.actorOf(
				Aggregator.props(), "agg-1");
		aggregatorTestRef.tell(new Aggregator.SOF(filePath),parent.ref());
		// check for the first Message
		aggregatorTestRef.tell(new Aggregator.Status(),parent.ref());
		parent.expectMsg(1);
		//check the word count calculation
		aggregatorTestRef.tell(new Aggregator.Line(filePath, lineContent), parent.ref());
		aggregatorTestRef.tell(new Aggregator.EOF(filePath), parent.ref());
		parent.expectMsg(4);
		//check the map after EOF message
		aggregatorTestRef.tell(new Aggregator.Status(),parent.ref());
		parent.expectMsg(1);
		parent.expectNoMsg();
	}
	
	@Test
	public void testInvalidMessageOrdering(){
		File file = new File("src/test/resources/home.txt");
		String absolutePath = file.getAbsolutePath();
		Path filePath = Paths.get(absolutePath);
		String lineContent = "home home home home";
		TestProbe parent = new TestProbe(system);
		final ActorRef aggregatorTestRef = system.actorOf(
				Aggregator.props(), "agg-2");
		
		// check the status before sending start of file message
		aggregatorTestRef.tell(new Aggregator.Status(),parent.ref());
		parent.expectMsg(0);
		//send line message before SOF
		aggregatorTestRef.tell(new Aggregator.Line(filePath, lineContent), parent.ref());
		parent.expectNoMsg();
		//send EOF
		aggregatorTestRef.tell(new Aggregator.EOF(filePath), parent.ref());
		parent.expectNoMsg();
	}
}
