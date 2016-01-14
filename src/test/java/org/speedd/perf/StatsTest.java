package org.speedd.perf;

import java.io.FileInputStream;

import org.junit.Test;
import static org.junit.Assert.*;

public class StatsTest {
	@Test
	public void latencyTest() throws Exception {
		Stats stats = Stats.analyze(new FileInputStream(getClass().getClassLoader().getResource("out-decisions").getFile()));

		assertNotNull(stats);
		
		assertEquals(101, stats.getLatency(0.9f));
	}

}
