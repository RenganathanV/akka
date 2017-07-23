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

import com.sample.assignment.actor.FileParser;
import com.sample.assignment.actor.FileScanner;

public class TestFileScanner {

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
	public void testFileScannerMessageForward() {
		File file = new File("src/test/resources/home.txt");
		String absolutePath = file.getAbsolutePath();
		Path resourceDirectory = Paths.get(absolutePath).getParent();
		TestProbe child = new TestProbe(system);
		final ActorRef fileScanner = system.actorOf(
				FileScanner.props(child.ref()), "file-scanner-1");
		fileScanner.tell(new FileScanner.Scan(resourceDirectory), child.ref());
		// check for the first Message
		child.expectMsgClass(FileParser.Parse.class);
		// no more messages
		child.expectNoMsg();
	}

	@Test
	public void testFileScannerUnhandledMessage() {
		TestProbe child = new TestProbe(system);
		final ActorRef fileScanner = system.actorOf(
				FileScanner.props(child.ref()), "file-scanner-2");
		// sending wrong message type
		fileScanner.tell(new FileParser.Parse(null), child.ref());
		child.expectMsg(Duration.create(3, TimeUnit.MILLISECONDS),
				"Unhandled Message Type");
	}

	@Test
	public void testInvalidDirectoryLocation() {
		// invalid location
		Path resourceDirectory = Paths.get("/dummy");
		TestProbe child = new TestProbe(system);
		final ActorRef fileScanner = system.actorOf(
				FileScanner.props(child.ref()), "file-scanner-3");
		fileScanner.tell(new FileScanner.Scan(resourceDirectory), child.ref());
		child.expectNoMsg(Duration.create(3, TimeUnit.MILLISECONDS));
	}
}
