package no.uio.ifi;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TracingFramework implements Serializable {
	static volatile List<TraceTuple> traceTuples;
	static volatile List<Integer> tracepoints;
	String trace_filename = null;

	public TracingFramework() {
		traceTuples = new ArrayList<>();
		tracepoints = new ArrayList<>();
	}

	public void addTracepoint(Integer traceID) {
		tracepoints.add(traceID);
	}

	public void writeTraceToFile(String trace_output_folder, String prefix_fn) {
		PrintWriter writer;
		try {
			if (this.trace_filename == null) {
				this.trace_filename = trace_output_folder.replaceFirst("^~", System.getProperty("user.home")) + "/" + prefix_fn + "-" + System.nanoTime() + "-" + UUID.randomUUID() + ".trace";
			}
			File file = new File(this.trace_filename);
			FileWriter fw = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(fw);
			writer = new PrintWriter(bw);
			synchronized (traceTuples) {
				for (TraceTuple tuple : traceTuples) {
					writer.println(tuple);
				}
				writer.close();
				traceTuples.clear();
			}
			System.out.println("Trace output written to " + trace_filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void traceEvent(int tracepointID, Object []args) {
		if (!tracepoints.contains(tracepointID))
			return;
		TraceTuple t = new TraceTuple(System.nanoTime(), tracepointID, args);
		traceTuples.add(t);
	}

	public synchronized void traceEvent(int tracepointID) {
		if (!tracepoints.contains(tracepointID))
			return;
		TraceTuple t = new TraceTuple(System.nanoTime(), tracepointID, new Object[0]);
		synchronized (traceTuples) {
			traceTuples.add(t);
		}
	}

	static class TraceTuple implements Serializable {
		long timestamp;
		int tracepointID;
		Object[] optional_arguments;

		TraceTuple(long timestamp, int tracepointID, Object[] optional_arguments) {
			this.timestamp = timestamp;
			this.tracepointID = tracepointID;
			this.optional_arguments = optional_arguments;
		}

		@Override
		public String toString() {
			// CPU ID is ignored
			StringBuilder ret = new StringBuilder(tracepointID + "\t" + timestamp);
			for (Object optional_argument: optional_arguments) {
				ret.append("\t").append(optional_argument.toString());
			}
			return ret.toString();
		}
	}
}
