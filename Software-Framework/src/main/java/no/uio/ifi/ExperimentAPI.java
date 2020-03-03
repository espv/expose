package no.uio.ifi;

import java.util.List;
import java.util.Map;

public interface ExperimentAPI {
	String Configure();
	String SetTupleBatchSize(int size);
	String SetIntervalBetweenTuples(int interval);
	String AddTuples(Map<String, Object> tuple, int quantity);
	String AddDataset(Map<String, Object> ds);
	String ProcessDataset(Map<String, Object> ds);
	String AddStreamSchemas(List<Map<String, Object>> stream_schema);
	String AddQueries(Map<String, Object> query);
	String AddSubscriberOfStream(int streamId, int nodeId);
	String SetNodeIdToAddress(Map<Integer, Map<String, Object> > newNodeIdToIpAndPort);
	String ProcessTuples(int number_tuples);
	String ClearQueries();
	String ClearTuples();
	String RunEnvironment();
	String StopEnvironment();
	String CleanupExperiment();
	String AddTracepointIds(List<Object> tracepointIds);
	String NotifyAfterNoReceivedTuple(int milliseconds);
	String TraceTuple(int tracepointId, List<String> arguments);
}
