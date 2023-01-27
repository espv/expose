package no.uio.ifi;

import java.util.List;
import java.util.Map;

public interface ExperimentAPI {
	void LockExecution();
	void UnlockExecution();

	String Configure();
	String SetTupleBatchSize(int size);
	String SetIntervalBetweenTuples(int interval);
	String SetTuplesPerSecondLimit(int tuples_per_second, List<Integer> node_id_list);
	String SendDsAsStream(Map<String, Object> ds, int iterations, boolean realism);
	String SendDsAsVariableOnOffStream(Map<String, Object> ds, int desired_tuples_per_second, int downtime, int min, int max, int step);
	String SendDsAsConstantStream(Map<String, Object> ds, int desired_tuples_per_second);
	String AddSchemas(List<Map<String, Object>> stream_schema);
	String DeployQueries(Map<String, Object> query);
	String AddNextHop(List<Integer> streamId_list, List<Integer> nodeId_list);
	String SetNidToAddress(Map<Integer, Map<String, Object> > newNodeIdToIpAndPort);
	String ClearQueries();
	String StartRuntimeEnv();
	String StopRuntimeEnv();
	String EndExperiment();
	String AddTpIds(List<Object> tracepointIds);
	String RetEndOfStream(int milliseconds);
	String RetReceivedXTuples(int number_tuples);
	String Wait(int milliseconds);
	String WriteStreamToCsv(int stream_id, String csvFilename);
	String TraceTuple(int tracepointId, List<String> arguments);
	String LoadSavepoint(String absolute_path);
	String MoveQueryState(int new_host);
	String MoveStaticQueryState(int query_id, int new_host);
	String MoveDynamicQueryState(int query_id, int new_host);
	String ResumeStream(List<Integer> stream_id_list);
	String StopStream(List<Integer> stream_id_list, int migration_coordinator_node_id);
	String BufferStream(List<Integer> stream_id_list);
	//String BufferAndStopStream(List<Integer> stream_id_list);
	//String BufferStopAndRelayStream(List<Integer> stream_id_list, List<Integer> old_host_list, List<Integer> new_host_list);
	String RelayStream(List<Integer> stream_id_list, List<Integer> old_host_list, List<Integer> new_host_list);
	String RemoveNextHop(List<Integer> stream_id_list, List<Integer> host_list);
	String AddSourceNodes(int query_id, List<Integer> stream_id_list, List<Integer> node_id_list);
	String SetAsPotentialHost(List<Integer> stream_id_list);
	String WaitForStoppedStreams(List<Integer> node_id_list, List<Integer> stream_id_list);
	Map<String, Object> CollectMetrics(long metrics_window);
}
