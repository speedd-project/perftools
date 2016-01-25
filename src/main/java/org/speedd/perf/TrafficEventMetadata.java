package org.speedd.perf;

import java.util.Map;

import org.speedd.EventParser;
import org.speedd.data.Event;
import org.speedd.data.impl.SpeeddEventFactory;
import org.speedd.traffic.TrafficAggregatedReadingCsv2Event;

public class TrafficEventMetadata implements EventMetadata {
	private static final TrafficAggregatedReadingCsv2Event parser = new TrafficAggregatedReadingCsv2Event(SpeeddEventFactory.getInstance());

	@Override
	public EventParser getEventParser() {
		return parser;
	}

	@Override
	public String getEventId(Event event) {
		Map<String, Object> attrs = event.getAttributes();
		
		return String.format("%s_%s_%s", attrs.get(TrafficAggregatedReadingCsv2Event.ATTR_LOCATION), attrs.get(TrafficAggregatedReadingCsv2Event.ATTR_LANE), attrs.get(TrafficAggregatedReadingCsv2Event.ATTR_TIMESTAMP));
	}

}
