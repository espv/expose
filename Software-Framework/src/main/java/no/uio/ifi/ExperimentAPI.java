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
	String MoveQueryState(int query_id, int new_host);
	String MoveStaticQueryState(int query_id, int new_host);
	String MoveDynamicQueryState(int query_id, int new_host);
	String ResumeStream(int stream_id);
	String StopStream(int stream_id);
	String BufferStream(int stream_id);
	String StopAndBufferStream(int stream_id);
	String RelayStream(int stream_id, int old_host, int new_host);
	String RemoveNextHop(int stream_id, int host);
	String AddSourceNode(int query_id, int stream_id, List<Integer> node_id_list);
}
