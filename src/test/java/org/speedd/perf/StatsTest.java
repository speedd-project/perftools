package org.speedd.perf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;

import org.junit.Test;

public class StatsTest {
	@Test
	public void statsTest() throws Exception {
		Stats stats = Stats.analyze(new FileInputStream(getClass().getClassLoader().getResource("eventslog").getFile()), 0, new CCFEventMetadata());

		assertNotNull(stats);
		
		assertEquals(361, stats.getLatency(0.9f));
		
		assertEquals(13, stats.getProcessingLatency(0.9f));
		
		assertEquals(48, stats.getNumOfInEvents());
		
		assertEquals(3.0, stats.getAvgInRate(), 1E-4);
	}
	
}
