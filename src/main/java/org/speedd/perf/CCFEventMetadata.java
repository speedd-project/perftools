package org.speedd.perf;

import org.speedd.EventParser;
import org.speedd.data.Event;
import org.speedd.data.impl.SpeeddEventFactory;
import org.speedd.fraud.Constants;
import org.speedd.fraud.FraudAggregatedReadingCsv2Event;

public class CCFEventMetadata implements EventMetadata {
	private static final FraudAggregatedReadingCsv2Event parser = new FraudAggregatedReadingCsv2Event(SpeeddEventFactory.getInstance());
	
	@Override
	public EventParser getEventParser() {
		return parser;
	}

	@Override
	public String getEventId(Event event) {
		return (String)event.getAttributes().get(Constants.ATTR_TRANSACTION_ID);
	}

}
