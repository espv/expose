package no.uio.ifi;

import java.io.Serializable;
import java.util.Map;

public class Task implements Serializable {
	Map<String, Object> event;
	Task next;

	Task() {}

	Task(Map<String, Object> event) {
		this.event = event;
	}

	public void setEvent(Map<String, Object> event) {this.event = event;}
	public Map<String, Object> getEvent() { return this.event; }
	public void setNext(Task next) { this.next = next; }
	public Task getNext() { return this.next; }
}
