package com.sample.assignment.actor;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;

import com.sample.assignment.actor.Aggregator;
import com.sample.assignment.actor.FileParser;

public class TestFileParser {

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
	public void testFileParserMessageForward() {
		File file = new File("src/test/resources/home.txt");
		String absolutePath = file.getAbsolutePath();
		Path resourceDirectory = Paths.get(absolutePath);
		TestProbe child = new TestProbe(system);
		final ActorRef fileParser = system.actorOf(
				FileParser.props(child.ref()), "file-parser-1");
		fileParser.tell(new FileParser.Parse(resourceDirectory), child.ref());
		// check for the first Message
		child.expectMsgClass(Aggregator.SOF.class);
		child.expectMsgClass(Aggregator.Line.class);
		child.expectMsgClass(Aggregator.EOF.class);
		// no more messages
		child.expectNoMsg();
	}

}
