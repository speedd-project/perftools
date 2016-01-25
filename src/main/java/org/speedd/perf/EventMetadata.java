package org.speedd.perf;

import org.speedd.EventParser;
import org.speedd.data.Event;

public interface EventMetadata {
	public EventParser getEventParser();
	public String getEventId(Event event);
}
