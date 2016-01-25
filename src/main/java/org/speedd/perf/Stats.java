package org.speedd.perf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.speedd.EventParser;
import org.speedd.data.Event;
import org.speedd.data.impl.SpeeddEventFactory;
import org.speedd.fraud.Constants;
import org.speedd.fraud.FraudAggregatedReadingCsv2Event;
import org.speedd.kafka.JsonEventDecoder;
import org.speedd.traffic.TrafficAggregatedReadingCsv2Event;

public class Stats {
	public static final String USAGE = "USAGE: stats -p <percentile> [-s <start offset timestamp>] [-f <file>]";
	private static final String OPTION_PERCENTILE = "p";
	private static final String OPTION_IN_FILE = "f";
	private static final String OPTION_START_OFFSET = "s";
	private static final String OPTION_USE_CASE = "c";
	
	private ArrayList<Long> e2eLatencies;
	
	private ArrayList<Long> processingLatencies;
	
	private long earliestInEventTimestamp;
	
	private long latestInEventTimestamp;
	
	private long numOfInEvents;
	
	private long startTimestamp;
	
	private EventMetadata eventMetadata;
	
	protected EventMetadata getEventMetadata() {
		return eventMetadata;
	}

	protected void setEventMetadata(EventMetadata eventMetadata) {
		this.eventMetadata = eventMetadata;
	}

	private JsonEventDecoder jsonParser;
	
	//map of timestamps by event id
	private Map<String, Long> timestamps;
	
	protected Stats(){
		earliestInEventTimestamp = 0;
		latestInEventTimestamp = 0;
		numOfInEvents = 0;
		startTimestamp = 0;
		timestamps = new HashMap<String, Long>();
		
		jsonParser = new JsonEventDecoder();
	}
	
	public static Stats analyze(InputStream eventStream, long startTimestamp, EventMetadata eventMetadata) throws IOException {
		Stats stats = new Stats();
		
		stats.setEventMetadata(eventMetadata);
		
		stats.setStartTimestamp(startTimestamp);
		
		stats.computeStats(eventStream);

		return stats;
	}
	
	protected void setStartTimestamp(long start){
		startTimestamp = start;
	}
	
	protected long getStartTimestamp(){
		return startTimestamp;
	}
	
	public long getLatency(float percentile){
		int percentileIndex = (int) (long) Math.round(e2eLatencies.size() * percentile)-1;
		
		return e2eLatencies.get(percentileIndex);
	}
	
	public long getProcessingLatency(float percentile){
		int percentileIndex = (int) (long) Math.round(processingLatencies.size() * percentile)-1;
		
		return processingLatencies.get(percentileIndex);
	}

	public double getAvgInRate(){
		return 1000 * (numOfInEvents - 1) / (latestInEventTimestamp - earliestInEventTimestamp);
	}
	
	private void updateLatencies(Event event, long eventTimestamp){
		Object[] contributingEvents = (Object[])event.getAttributes().get("transaction_ids");

		if(contributingEvents.length == 0){
			return;
		}
		
		long latestContributingInEventTimestamp = 0;
		
		for(int i=contributingEvents.length-1; i>=0; --i){
			String eventId = (String)contributingEvents[i];
			
			if(timestamps.containsKey(eventId)){
				latestContributingInEventTimestamp = timestamps.get(eventId);
				break;
			}
		}

		if(latestContributingInEventTimestamp > 0){
			Long latency = eventTimestamp - latestContributingInEventTimestamp;
			
			e2eLatencies.add(latency);
		}

		//update processing latencies
		long internalTimestamp = event.getTimestamp();
		Object[] contributingTimestamps = (Object[])event.getAttributes().get("timestamps");
		Object latestContributingTimestampObject = contributingTimestamps[contributingTimestamps.length-1];
		long latestContributingInternalTimestamp;
		if(latestContributingTimestampObject instanceof String){
			latestContributingInternalTimestamp = Long.parseLong((String)latestContributingTimestampObject);
		} else {
			latestContributingInternalTimestamp = (Long)latestContributingTimestampObject;
		}
		
		Long internalLatency = internalTimestamp - latestContributingInternalTimestamp;
		processingLatencies.add(internalLatency);
	}

	private void updateInEventMetrics(Event event, long timestamp){
		numOfInEvents++;
		
		timestamps.put(getEventId(event), timestamp);
		
		if(earliestInEventTimestamp == 0){
			earliestInEventTimestamp = timestamp;
			latestInEventTimestamp = timestamp;
		} else {
			if(earliestInEventTimestamp > timestamp){
				earliestInEventTimestamp = timestamp;
			}
			
			if(latestInEventTimestamp < timestamp){
				latestInEventTimestamp = timestamp;
			}
		}
	}

	protected String getEventId(Event event){
		Map<String, Object> attrs = event.getAttributes();
		
		if(attrs.containsKey("EventId")){
			return (String)attrs.get("EventId");
		} else { 
			//FIXME parameterize to support other use cases/events, not just CC transactions
			return (String)attrs.get(Constants.ATTR_TRANSACTION_ID);
		}
	}
	
	protected boolean isJSON(String str){
		return str != null && str.trim().startsWith("{");
	}
	
	protected void computeStats(InputStream eventStream) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(eventStream));
		
		boolean done = false;
		
		e2eLatencies = new ArrayList<Long>();
		
		processingLatencies = new ArrayList<Long>();
		
		JsonEventDecoder decoder = new JsonEventDecoder();
		
		do {
			String line = reader.readLine();
			if(line == null){
				done = true;
				continue;
			}
			
			String[] eventEntry = line.split(":", 2);
			long timestamp = Long.parseLong(eventEntry[0].trim());
			
			if(timestamp < startTimestamp){
				continue;
			}
			
			if(isJSON(eventEntry[1])){
				Event event = decoder.fromBytes(eventEntry[1].trim().getBytes());
				
				if(event.getAttributes().containsKey("timestamps")){
					updateLatencies(event, timestamp);
				} 
			} else {
				//input (csv) event
					//input event - does not contain 'timestamps' - use to compute real rates
				Event event = eventMetadata.getEventParser().fromBytes(eventEntry[1].trim().getBytes());
				updateInEventMetrics(event, timestamp);
			}
			
			
		} while (!done);
		
		Collections.sort(e2eLatencies);
		Collections.sort(processingLatencies);
		
	}
	
	public long getNumOfInEvents(){
		return numOfInEvents;
	}
	
	public static void main(String[] args) {
		Options options = new Options();

		options.addOption(Option.builder(OPTION_PERCENTILE).hasArg().type(Float.class).required().build());
		options.addOption(Option.builder(OPTION_IN_FILE).hasArg().build());
		options.addOption(Option.builder(OPTION_START_OFFSET).required(false).hasArg().build());
		options.addOption(Option.builder(OPTION_USE_CASE).required(false).hasArg().build());

		try {
			CommandLineParser clParser = new DefaultParser();
			CommandLine cmd = clParser.parse(options, args);
			
			float percentile = Float.parseFloat(cmd.getOptionValue(OPTION_PERCENTILE));
			
			if(percentile <= 0 || percentile > 1){
				throw new ParseException("Percentile value must be within (0,1]");
			}
			
			long startOffset = cmd.hasOption(OPTION_START_OFFSET)? Long.valueOf(cmd.getOptionValue(OPTION_START_OFFSET)) : 0;
			
			InputStream in = null;
			
			if(cmd.hasOption(OPTION_IN_FILE)){
				String path = cmd.getOptionValue(OPTION_IN_FILE);
				in = new FileInputStream(path);
			} else {
				in = System.in;
			}
			
			EventMetadata eventMetadata;
			
			if(cmd.hasOption(OPTION_USE_CASE) && cmd.getOptionValue(OPTION_USE_CASE).equals("traffic")){
				eventMetadata = new TrafficEventMetadata();
			} else {
				eventMetadata = new CCFEventMetadata();
			}
			
			Stats stats = Stats.analyze(in, startOffset, eventMetadata);
			
			System.out.println(String.format("%.1f%% End-to-end latency: %d ms", percentile * 100, stats.getLatency(percentile)));
			System.out.println(String.format("%.1f%% Processing latency: %d ms", percentile * 100, stats.getProcessingLatency(percentile)));
			System.out.println(String.format("Num of input events: %d, average rate: %f events/sec", stats.numOfInEvents, stats.getAvgInRate() ));
			
		} catch (ParseException pe){
			System.err.println(pe.getMessage());
			System.err.println(USAGE);
			System.exit(1);
		} catch (IOException ioe){
			System.out.println(ioe.getMessage());
			System.exit(1);
		}
	}

}
