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
	public static final String USAGE = "USAGE: stats -p <percentile> [-f <file>]";
	private static final String OPTION_PERCENTILE = "p";
	private static final String OPTION_IN_FILE = "f";
	
	private ArrayList<Long> latencies;
	
	protected Stats(){
		
	}
	
	public static Stats analyze(InputStream eventStream) throws IOException {
		Stats stats = new Stats();
		
		stats.computeLatencies(eventStream);
		return stats;
	}
	
	public long getLatency(float percentile){
		int percentileIndex = (int) (long) Math.round(latencies.size() * percentile);
		
		return latencies.get(percentileIndex);
	}
	
	protected void computeLatencies(InputStream eventStream) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(eventStream));
		
		boolean done = false;
		
		latencies = new ArrayList<Long>();
		
		int nValues = 0;

		JsonEventDecoder decoder = new JsonEventDecoder();

		do {
			String line = reader.readLine();
			if(line == null){
				done = true;
				continue;
			}
			
			Event event = decoder.fromBytes(line.getBytes());
			
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
		} while (!done);
		
		Collections.sort(latencies);
		
	}
	
	public static void main(String[] args) {
		Options options = new Options();

		options.addOption(Option.builder(OPTION_PERCENTILE).hasArg().type(Float.class).required().build());
		options.addOption(Option.builder(OPTION_IN_FILE).hasArg().build());

		try {
			CommandLineParser clParser = new DefaultParser();
			CommandLine cmd = clParser.parse(options, args);
			
			float percentile = Float.parseFloat(cmd.getOptionValue(OPTION_PERCENTILE));
			
			if(percentile <= 0 || percentile > 1){
				throw new ParseException("Percentile value must be within (0,1]");
			}
			
			InputStream in = null;
			
			if(cmd.hasOption(OPTION_IN_FILE)){
				String path = cmd.getOptionValue(OPTION_IN_FILE);
				in = new FileInputStream(path);
			} else {
				in = System.in;
			}
			
			Stats stats = Stats.analyze(in);
			
			System.out.println(String.format("%.1f%% latency: %d ms", percentile, stats.getLatency(percentile)));

			
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
