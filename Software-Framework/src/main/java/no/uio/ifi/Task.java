package no.uio.ifi;

import java.io.Serializable;
import java.util.Map;

class Task implements Serializable {
	Map<String, Object> event;

	Task(Map<String, Object> event) {
		this.event = event;
	}
}
