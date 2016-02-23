package org.speedd.perf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.speedd.data.Event;
import org.speedd.data.impl.SpeeddEventFactory;
import org.speedd.fraud.Constants;
import org.speedd.fraud.FraudAggregatedReadingCsv2Event;

/**
 * Fields to generate unique values for: - transaction_id: string - card_pan:
 * string - terminal_id: string - acquirer_country: integer - card_country:
 * integer
 * 
 * @author kofman
 * 
 */
public class TransactionEventGenerator implements Constants {
	public static interface EventConsumer {
		public void onEvent(Event event);
	}

	public static class FileWriter implements EventConsumer {
		private File outfile;
		private PrintWriter writer;
		private boolean isOpen;

		public FileWriter(File file) {
			outfile = file;
			isOpen = false;
		}

		public void onEvent(Event event) {
			FraudAggregatedReadingCsv2Event serializer = new FraudAggregatedReadingCsv2Event(
					SpeeddEventFactory.getInstance());
			writer.println(new String(serializer.toBytes(event)));
		}

		public void open() throws FileNotFoundException {
			if (!isOpen) {
				writer = new PrintWriter(outfile);
				isOpen = true;
			}
		}

		public void close() {
			writer.flush();
			writer.close();
			writer = null;
			isOpen = false;
		}

	}

	private int nReps;

	private int eventReadCount;

	private HashMap<String, HashMap<Object, ArrayList>> fields;

	private static final String[] uniqueAttrs = new String[] {
			ATTR_TRANSACTION_ID,
			ATTR_CARD_PAN,
			ATTR_TERMINAL_ID,
			ATTR_ACQUIRER_COUNTRY,
			ATTR_CARD_COUNTRY
	};

	private static final HashMap<String, String> uniqueAttrMap = new HashMap<String, String>(
			uniqueAttrs.length);

	private static final String OPTION_SRC_FILE = "s";

	private static final String OPTION_TGT_FILE = "t";

	private static final String OPTION_NREPS = "r";

	private static final String USAGE = "USAGE: genevents -s <src-file> -t <tgt-file> -r <num of repetitions>";
	static {
		for (String attrName : uniqueAttrs) {
			uniqueAttrMap.put(attrName, attrName);
		}
	}

	public TransactionEventGenerator() {
		fields = new HashMap<String, HashMap<Object, ArrayList>>();
		fields.put(ATTR_TRANSACTION_ID, new HashMap<Object, ArrayList>());
		fields.put(ATTR_CARD_PAN, new HashMap<Object, ArrayList>());
		fields.put(ATTR_TERMINAL_ID, new HashMap<Object, ArrayList>());
		fields.put(ATTR_ACQUIRER_COUNTRY, new HashMap<Object, ArrayList>());
		fields.put(ATTR_CARD_COUNTRY, new HashMap<Object, ArrayList>());
	}

	public void initialize(String path, int nReps) throws IOException {
		BufferedReader reader = null;

		this.nReps = nReps;

		try {
			FraudAggregatedReadingCsv2Event decoder = new FraudAggregatedReadingCsv2Event(
					SpeeddEventFactory.getInstance());

			reader = new BufferedReader(new FileReader(path));

			boolean done = false;

			eventReadCount = 0;
			do {
				String line = reader.readLine();
				if (line == null) {
					done = true;
					continue;
				}

				eventReadCount++;

				Event event = decoder.fromBytes(line.getBytes());

				for (String attrName : uniqueAttrs) {
					generateValuesForAttribute(attrName, event, nReps);
				}

			} while (!done);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	private void generateValuesForAttribute(String attrName, Event event,
			int nReps) {
		Object attrValue = event.getAttributes().get(attrName);

		HashMap<Object, ArrayList> values = fields.get(attrName);
		ArrayList generatedValues = values.get(attrValue);

		if (generatedValues != null) {
			return;
		}

		generatedValues = new ArrayList(nReps);
		generateValues(attrValue, generatedValues, nReps);

		values.put(attrValue, generatedValues);
	}

	private void generateStrings(ArrayList generatedValues, int count) {
		for (int i = 0; i < count; ++i) {
			generatedValues.add(UUID.randomUUID().toString());
		}
	}

	private void generateIntegers(ArrayList generatedValues, int count) {
		String prefix = String.valueOf(1000 + eventReadCount);
		for (int i = 1; i <= count; ++i) {
			String valueStr = prefix + String.valueOf(i);
			generatedValues.add(Integer.valueOf(valueStr));
		}
	}

	private void generateValues(Object sourceValue, ArrayList generatedValues,
			int nReps) {
		if (sourceValue instanceof String) {
			generateStrings(generatedValues, nReps);
		} else { // integers
			generateIntegers(generatedValues, nReps);
		}
	}

	public void generateEvents(String sourceEventsPath,
			EventConsumer eventConsumer) throws IOException {
		BufferedReader reader = null;

		FraudAggregatedReadingCsv2Event decoder = new FraudAggregatedReadingCsv2Event(
				SpeeddEventFactory.getInstance());

		for (int i = 0; i < nReps; ++i) {
			try {
				reader = new BufferedReader(new FileReader(sourceEventsPath));

				boolean done = false;

				do {
					String line = reader.readLine();
					if (line == null) {
						done = true;
						continue;
					}

					Event event = decoder.fromBytes(line.getBytes());

					for (String attrName : uniqueAttrs) {
						replaceUniqueAttr(event, attrName, i);
					}

					eventConsumer.onEvent(event);

				} while (!done);
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
		}
	}

	private void replaceUniqueAttr(Event event, String attrName, int rep) {
		event.getAttributes().put(
				attrName,
				fields.get(attrName).get(event.getAttributes().get(attrName))
						.get(rep));
	}

	boolean isUniqueAttr(String attrName) {
		return uniqueAttrMap.containsKey(attrName);
	}

	/**
	 * USAGE: genevents -s <src-file> -t <tgt-file> -r <num of repetitions>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Options options = new Options();

		options.addOption(Option.builder(OPTION_SRC_FILE).hasArg().required().build());
		options.addOption(Option.builder(OPTION_TGT_FILE).hasArg().required().build());
		options.addOption(Option.builder(OPTION_NREPS).hasArg().build());

		try {
			CommandLineParser clParser = new DefaultParser();
			CommandLine cmd = clParser.parse(options, args);

			String srcPath = cmd.getOptionValue(OPTION_SRC_FILE);

			String tgtPath = cmd.getOptionValue(OPTION_TGT_FILE);

			int nReps = cmd.hasOption(OPTION_NREPS) ? Integer.parseInt(cmd
					.getOptionValue(OPTION_NREPS)) : 1;

			System.out.println(String.format("Generating %d replicas of %s - writing to %s", nReps, srcPath, tgtPath));
			FileWriter fileWriter = new FileWriter(new File(tgtPath));

			TransactionEventGenerator gen = new TransactionEventGenerator();
			
			gen.initialize(srcPath, nReps);

			fileWriter.open();

			gen.generateEvents(srcPath, fileWriter);

			fileWriter.close();
			
			System.out.println("Completed.");
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			System.err.println(USAGE);
			System.exit(1);
		} catch (IOException ioex) {
			System.err.println("Error: " + ioex.getMessage());
			System.exit(1);
		}

	}
}
