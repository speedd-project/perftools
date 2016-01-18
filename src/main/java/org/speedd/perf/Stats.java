package org.speedd.perf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.speedd.data.Event;
import org.speedd.kafka.JsonEventDecoder;

public class Stats {
	public static final String USAGE = "USAGE: stats -p <percentile> [-s <start offset timestamp>] [-f <file>]";
	private static final String OPTION_PERCENTILE = "p";
	private static final String OPTION_IN_FILE = "f";
	private static final String OPTION_START_OFFSET = "s";
	
	private ArrayList<Long> latencies;
	
	private ArrayList<Long> inEventDelays;
	
	private long earliestInEventTimestamp;
	
	private long latestInEventTimestamp;
	
	private long numOfInEvents;
	
	private double avgInEventRate;
	
	private long startTimestamp;
	
	
	protected Stats(){
		earliestInEventTimestamp = 0;
		latestInEventTimestamp = 0;
		numOfInEvents = 0;
		avgInEventRate = 0;
		startTimestamp = 0;
	}
	
	public static Stats analyze(InputStream eventStream, long startTimestamp) throws IOException {
		Stats stats = new Stats();
		
		stats.setStartTimestamp(startTimestamp);
		
		stats.computeLatencies(eventStream);
		return stats;
	}
	
	protected void setStartTimestamp(long start){
		startTimestamp = start;
	}
	
	protected long getStartTimestamp(){
		return startTimestamp;
	}
	
	public long getLatency(float percentile){
		int percentileIndex = (int) (long) Math.round(latencies.size() * percentile)-1;
		
		return latencies.get(percentileIndex);
	}
	
	public double getAvgInRate(){
		return 1000 * (numOfInEvents - 1) / (latestInEventTimestamp - earliestInEventTimestamp);
	}
	
	private void updateLatencies(Event event){
		long eventTimestamp = event.getTimestamp();

		Object[] timestamps = (Object[])event.getAttributes().get("timestamps");
		
		Long[] ts = new Long[timestamps.length];
		
		if(timestamps instanceof String[]){
			String[] strTS = (String[])timestamps;
			
			for (int i=0; i<strTS.length; ++i) {
				ts[i] = Long.valueOf(strTS[i]);
			}
		} else {
			ts = (Long[])timestamps;
		}

		Arrays.sort(ts);
		
		Long latestInTS = ts[ts.length-1];
		Long latency = eventTimestamp - latestInTS;
		
		latencies.add(latency);
	}

	private void updateInEventMetrics(Event event){
		long timestamp = event.getTimestamp();
		
		numOfInEvents++;
		
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
	
	protected void computeLatencies(InputStream eventStream) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(eventStream));
		
		boolean done = false;
		
		latencies = new ArrayList<Long>();
		
		JsonEventDecoder decoder = new JsonEventDecoder();

		do {
			String line = reader.readLine();
			if(line == null){
				done = true;
				continue;
			}
			
			Event event = decoder.fromBytes(line.getBytes());
			
			//skip events earlier than the start offset timestamp
			if(event.getTimestamp() < startTimestamp){
				continue;
			}
			
			long eventTimestamp = event.getTimestamp();
			
			if(event.getAttributes().containsKey("timestamps")){
				updateLatencies(event);
			} else {
				//input event - does not contain 'timestamps' - use to compute real rates
				updateInEventMetrics(event);
			}
			
		} while (!done);
		
		Collections.sort(latencies);
		
	}
	
	public long getNumOfInEvents(){
		return numOfInEvents;
	}
	
	public static void main(String[] args) {
		Options options = new Options();

		options.addOption(Option.builder(OPTION_PERCENTILE).hasArg().type(Float.class).required().build());
		options.addOption(Option.builder(OPTION_IN_FILE).hasArg().build());
		options.addOption(Option.builder(OPTION_START_OFFSET).required(false).hasArg().build());

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
			
			Stats stats = Stats.analyze(in, startOffset);
			
			System.out.println(String.format("%.1f%% latency: %d ms", percentile, stats.getLatency(percentile)));
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
