package org.speedd.perf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLoggerFactory;

public class Analyzer {
	private static final String OPTION_ZK_CONNECT = "zookeeper";
	private static final String OPTION_TOPICS = "topics";
	private static final String OPTION_THREADS_PER_TOPIC = "threads";
	private static final String USAGE = "analyzer --zookeeper=<host:port> --group=<group id> --topics=<name1,name2,...> --threads=<number>";
	private static final String OPTION_GROUP_ID = "group";
	private ConsumerConnector consumer;
	private String[] topics;
	private int threadsPerTopic;
	private ExecutorService executor;
	private ConcurrentLinkedQueue<String> queue;
	
	private static class EventWriter implements Runnable {
		private ConcurrentLinkedQueue<String> printQueue;
		public EventWriter(ConcurrentLinkedQueue<String> printQueue) {
			this.printQueue = printQueue;
		}
		@Override
		public void run() {
			while(true){
				String nextRecord = printQueue.poll();
				if(nextRecord != null){
					System.out.println(nextRecord);
				} else {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {}
				}
			}
		}	
	}
	
	private static class EventRecorder implements Runnable {
		private KafkaStream stream;
		private int threadId;
		private String topic;
		private ConcurrentLinkedQueue<String> printQueue;
		
		public EventRecorder(String topic, KafkaStream stream, int threadId, ConcurrentLinkedQueue<String> printQueue) {
			this.stream = stream;
			this.threadId = threadId;
			this.topic = topic;
			this.printQueue = printQueue;
		}

		@Override
		public void run() {
			System.err.println("Starting thread " + threadId + " for topic " + topic);
			
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while (it.hasNext()) {
	        	long timestamp = System.currentTimeMillis();
	        	String message = new String(it.next().message());
	        	
//	        	System.out.println(timestamp + ":" + message);
	        	printQueue.add(String.format("%d:%s", timestamp, message));
	        }
	            
	        System.err.println("Shutting down thread: " + threadId + " for topic " + topic);
		}
		
	}
	
	public Analyzer(String zkConnect, String[] topics, String groupId, int threadsPerTopic){
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zkConnect, groupId));
		this.topics = topics;
		this.threadsPerTopic = threadsPerTopic;
	}
	
    private static ConsumerConfig createConsumerConfig(String zkConnect, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
    
    public void run() {
    	Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    	
    	for (String topic : topics) {
    		topicCountMap.put(topic, new Integer(threadsPerTopic));
    	}
    	
    	Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        executor = Executors.newFixedThreadPool(topics.length * threadsPerTopic + 1);
        
        queue = new ConcurrentLinkedQueue<String>();
        
        executor.submit(new EventWriter(queue));

    	for (String topic : topics) {
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            
            int threadId = 0;
            
            for (KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new EventRecorder(topic, stream, threadId, queue));
				threadId++;
			}
		}
    	
    }
    
	public static void main(String[] args) {
		Options options = new Options();

		options.addOption(Option.builder().longOpt(OPTION_ZK_CONNECT).hasArg().required().build());
		options.addOption(Option.builder().longOpt(OPTION_GROUP_ID).hasArg().required().build());
		options.addOption(Option.builder().longOpt(OPTION_TOPICS).hasArg().build());
		options.addOption(Option.builder().longOpt(OPTION_THREADS_PER_TOPIC).required().type(Integer.class).hasArg().build());

		try {
			CommandLineParser clParser = new DefaultParser();
			CommandLine cmd = clParser.parse(options, args);
			
			String zkConnect = cmd.getOptionValue(OPTION_ZK_CONNECT);
			String groupId = cmd.getOptionValue(OPTION_GROUP_ID);
			String[] topics = cmd.getOptionValue(OPTION_TOPICS).split("\\s*,\\s*");
			int threadsPerTopic = Integer.valueOf(cmd.getOptionValue(OPTION_THREADS_PER_TOPIC));
			
			Analyzer analyzer = new Analyzer(zkConnect, topics, groupId, threadsPerTopic);
			analyzer.run();

		} catch (ParseException e){
			System.err.println(USAGE);
		}
	}

	
}
