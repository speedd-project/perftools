package org.speedd.perf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;

import org.junit.Test;

public class StatsTest {
	@Test
	public void statsTest() throws Exception {
		Stats stats = Stats.analyze(new FileInputStream(getClass().getClassLoader().getResource("out-decisions").getFile()), 0);

		assertNotNull(stats);
		
		assertEquals(8, stats.getLatency(0.9f));
		
		assertEquals(48, stats.getNumOfInEvents());
		
		assertEquals(3.0, stats.getAvgInRate(), 1E-4);
	}
	
	@Test
	public void statsTestWithOffset() throws Exception {
		long offset = 1452884299500L;
		
		Stats stats = Stats.analyze(new FileInputStream(getClass().getClassLoader().getResource("out-decisions").getFile()), offset);

		assertNotNull(stats);
		
		assertEquals(9, stats.getLatency(0.9f));
		
		assertEquals(41, stats.getNumOfInEvents());
		
		assertEquals(3.0, stats.getAvgInRate(), 1E-4);
	}

}
