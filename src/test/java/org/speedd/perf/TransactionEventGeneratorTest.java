package org.speedd.perf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;
import org.speedd.data.Event;
import org.speedd.data.impl.SpeeddEventFactory;
import org.speedd.fraud.Constants;
import org.speedd.fraud.FraudAggregatedReadingCsv2Event;
import org.speedd.perf.TransactionEventGenerator.FileWriter;

public class TransactionEventGeneratorTest {
	TransactionEventGenerator gen;
	
	@Test
	public void generateEventsTest() throws Exception {
		String srcPath = getClass().getClassLoader()
				.getResource("FeedzaiIntegrationData.csv").getPath();

		String tgtPath = "generated-events.csv";

		FileWriter fileWriter = new FileWriter(new File(tgtPath));

		gen = new TransactionEventGenerator();
		gen.initialize(srcPath, 1);

		fileWriter.open();

		gen.generateEvents(srcPath, fileWriter);

		fileWriter.close();

		verifyGeneratedEvents(readEventsFromCSV(srcPath),
				readEventsFromCSV(tgtPath));
	}

	private ArrayList<Event> readEventsFromCSV(String csvPath)
			throws IOException {
		ArrayList<Event> events = new ArrayList<Event>();
		BufferedReader reader = null;

		try {
			FraudAggregatedReadingCsv2Event csv2event = new FraudAggregatedReadingCsv2Event(
					SpeeddEventFactory.getInstance());

			reader = new BufferedReader(new FileReader(csvPath));

			boolean done = false;

			do {
				String line = reader.readLine();
				if (line == null) {
					done = true;
					continue;
				}

				Event event = csv2event.fromBytes(line.getBytes());
				events.add(event);
			} while (!done);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
		return events;
	}

	private void verifyGeneratedEvents(ArrayList<Event> srcEvents,
			ArrayList<Event> generatedEvents) {
		assertEquals(
				"Both source and generated arrays should be of equal size",
				srcEvents.size(), generatedEvents.size());

		for (int i = 0; i < srcEvents.size() - 1; ++i) {
			for (int j = i + 1; j < srcEvents.size(); ++j) {
				Event src1 = srcEvents.get(i);
				Event src2 = srcEvents.get(j);
				Event gen1 = generatedEvents.get(i);
				Event gen2 = generatedEvents.get(j);

				Map<String, Object> srcAttrs1 = src1.getAttributes();
				Map<String, Object> srcAttrs2 = src2.getAttributes();
				Map<String, Object> genAttrs1 = gen1.getAttributes();
				Map<String, Object> genAttrs2 = gen2.getAttributes();

				for (String attrName : srcAttrs1.keySet()) {
					if (gen.isUniqueAttr(attrName)) {
						assertTrue(
								String.format(
										"Values of %s should only be equal between generated events iff they are equal for source events",
										attrName),
								srcAttrs1.get(attrName).equals(
										srcAttrs2.get(attrName)) == genAttrs1
										.get(attrName).equals(
												genAttrs2.get(attrName)));
					} else if (!attrName.equals(Constants.ATTR_DETECTION_TIME)) {
						assertEquals(
								String.format(
										"Non-unique attribute values for %s should be equal in both source and generated events",
										attrName), srcAttrs1.get(attrName),
								genAttrs1.get(attrName));
						assertEquals(
								String.format(
										"Non-unique attribute values for %s should be equal in both source and generated events",
										attrName), srcAttrs2.get(attrName),
								genAttrs2.get(attrName));
					}
				}
			}
		}
	}

}
