package no.uio.ifi;

import java.util.List;
import java.util.Map;

public interface ExperimentAPI {
	String Configure();
	String SetTupleBatchSize(int size);
	String SetIntervalBetweenTuples(int interval);
	String SendDsAsStream(Map<String, Object> ds);
	String AddSchemas(List<Map<String, Object>> stream_schema);
	String DeployQueries(Map<String, Object> query);
	String AddNextHop(int streamId, int nodeId);
	String SetNidToAddress(Map<Integer, Map<String, Object> > newNodeIdToIpAndPort);
	String ClearQueries();
	String StartRuntimeEnv();
	String StopRuntimeEnv();
	String EndExperiment();
	String AddTpIds(List<Object> tracepointIds);
	String RetEndOfStream(int milliseconds);
	String WriteStreamToCsv(int stream_id, String csvFilename);
	String TraceTuple(int tracepointId, List<String> arguments);
	String LoopTasks(int numberIterations, List<Map<String, Object>> tasks);
}
