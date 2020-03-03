package no.uio.ifi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatorExperimentAPI implements ExperimentAPI {
	CoordinatorComm coordinatorComm;
	int node_id;

	CoordinatorExperimentAPI(CoordinatorComm coordinatorComm, int node_id) {
		this.coordinatorComm = coordinatorComm;
		this.node_id = node_id;
	}

	@Override
	public String Configure() {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "Configure");
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String SetTupleBatchSize(int size) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "SetTupleBatchSize");
		List<Object> args = new ArrayList<>();
		args.add(size);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String SetIntervalBetweenTuples(int interval) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "SetIntervalBetweenTuples");
		List<Object> args = new ArrayList<>();
		args.add(interval);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String AddTuples(Map<String, Object> tuple, int quantity) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "AddTuples");
		List<Object> args = new ArrayList<>();
		args.add(tuple);
		args.add(quantity);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String AddDataset(Map<String, Object> ds) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "AddDataset");
		List<Object> args = new ArrayList<>();
		args.add(ds);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String ProcessDataset(Map<String, Object> ds) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "ProcessDataset");
		List<Object> args = new ArrayList<>();
		args.add(ds);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String AddStreamSchemas(List<Map<String, Object>> stream_schemas) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "AddStreamSchemas");
		map.put("arguments", stream_schemas);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String AddQueries(Map<String, Object> query) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "AddQueries");
		List<Object> args = new ArrayList<>();
		args.add(query);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String AddSubscriberOfStream(int streamId, int nodeId) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "AddSubscriberOfStream");
		List<Object> args = new ArrayList<>();
		args.add(streamId);
		args.add(nodeId);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String SetNodeIdToAddress(Map<Integer, Map<String, Object> > newNodeIdToIpAndPort) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "SetNodeIdToAddress");
		List<Object> args = new ArrayList<>();
		args.add(newNodeIdToIpAndPort);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String ProcessTuples(int number_tuples) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "ProcessTuples");
		List<Object> args = new ArrayList<>();
		args.add(number_tuples);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String ClearQueries() {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "ClearQueries");
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String ClearTuples() {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "ClearTuples");
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String RunEnvironment() {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "RunEnvironment");
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String StopEnvironment() {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "StopEnvironment");
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String CleanupExperiment() {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "CleanupExperiment");
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String AddTracepointIds(List<Object> tracepointIds) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "AddTracepointIds");
		map.put("arguments", tracepointIds);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String NotifyAfterNoReceivedTuple(int nanoseconds) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "NotifyAfterNoReceivedTuple");
		List<Object> args = new ArrayList<>();
		args.add(nanoseconds);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}

	@Override
	public String TraceTuple(int tracepointId, List<String> traceArguments) {
		Map<String, Object> map = new HashMap<>();
		map.put("task", "TraceTuple");
		List<Object> args = new ArrayList<>();
		args.add(tracepointId);
		args.add(traceArguments);
		map.put("arguments", args);
		Task cmd = new Task(map);
		return coordinatorComm.SendToSpe(cmd, node_id);
	}
}
