package no.uio.ifi;

import org.apache.commons.cli.*;
import org.apache.commons.text.StringEscapeUtils;

import java.io.*;
import java.net.Socket;
import java.util.*;


public class SpeTaskHandler extends Comm implements MainTaskHandler {
	private String client_ip;
	int client_port;
	int node_id;
	String coordinator_ip;
	int coordinator_port;
	int spe_coordinator_port;
	int state_transfer_port = -1;
	ExperimentAPI experimentAPI;
	SpeSpecificAPI speSpecificAPI;
	String trace_folder;
	public NodeComm speNodeComm;

	final int MAIN_COORDINATOR_NODE_ID = 0;

	public SpeTaskHandler(String[] args, ExperimentAPI experimentAPI, SpeSpecificAPI speSpecificAPI) {
		Options options = new Options();
		Option cp = new Option("c", "client-port", true, "Client port");
		cp.setRequired(true);
		options.addOption(cp);

		Option ci = new Option("l", "client-ip", true, "Client IP");
		ci.setRequired(true);
		options.addOption(ci);

		Option ip = new Option("i", "coordinator-ip", true, "Coordinator IP");
		ip.setRequired(true);
		options.addOption(ip);

		Option port = new Option("p", "coordinator-port", true, "Coordinator port");
		port.setRequired(true);
		options.addOption(port);

		Option spe_coordinator_port = new Option("scp", "spe-coordinator-port", true, "SPE coordinator port");
		port.setRequired(true);
		options.addOption(spe_coordinator_port);

		Option node_id = new Option("n", "node-id", true, "Node ID");
		node_id.setRequired(true);
		options.addOption(node_id);

		Option t = new Option("t", "trace-output-folder", true, "Folder to place trace in");
		t.setRequired(true);
		options.addOption(t);

		Option c = new Option("o", "connector", true, "which connector to use for communication between nodes");
		c.setRequired(false);
		options.addOption(c);

		Option st = new Option("s", "state-transfer-port", true, "TCP port used for for TCP server on new host receiving operator state");
		st.setRequired(false);
		options.addOption(st);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("utility-name", options);

			System.exit(1);
		}

		this.client_port = Integer.parseInt(cmd.getOptionValue("client-port"));
		this.client_ip = cmd.getOptionValue("client-ip");
		this.node_id = Integer.parseInt(cmd.getOptionValue("node-id"));
		this.coordinator_ip = cmd.getOptionValue("coordinator-ip");
		this.coordinator_port = Integer.parseInt(cmd.getOptionValue("coordinator-port"));
		this.spe_coordinator_port = Integer.parseInt(cmd.getOptionValue("spe-coordinator-port"));
		if (cmd.hasOption("state-transfer-port")) {
			this.state_transfer_port = Integer.parseInt(cmd.getOptionValue("state-transfer-port"));
		}
		this.trace_folder = cmd.getOptionValue("trace-output-folder");
		this.experimentAPI = experimentAPI;
		this.speSpecificAPI = speSpecificAPI;

		speNodeComm = new NodeComm(this.client_ip, this.spe_coordinator_port, this.client_port, this.state_transfer_port, null, this.node_id);
		speNodeComm.mainTaskHandler = this;
		new Thread(speNodeComm).start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			ConnectToSpeCoordinator(MAIN_COORDINATOR_NODE_ID, coordinator_ip, coordinator_port);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(10);
		}
	}

	public void ConnectToSpeCoordinator(int spe_coordinator_node_id, String coordinator_ip, int coordinator_port) throws Exception {
		this.speNodeComm.ConnectToNode(spe_coordinator_node_id, coordinator_ip, coordinator_port);
	}

	public int GetStateTransferPort() {
		return state_transfer_port;
	}

	public String GetTraceOutputFolder() {return this.trace_folder;}

	public void ShutDown() {
		System.exit(0);
	}

	public int GetNodeId() {return this.node_id;}

	public int GetClientPort() {return this.client_port;}

	public String IssueTask(Map<String, Object> task, int node_id_to_execute) {
		System.out.println("Issuing task " + task  + " to Node " + node_id_to_execute);
		List<Integer> node_id_list = (List<Integer>) task.get("node");
		for (int node_id : node_id_list) {
			task.put("node", Arrays.asList(node_id));
			if ((boolean) task.getOrDefault("parallel", true)) {
				new Thread(() -> speNodeComm.SendToSpe(task)).start();
			} else {
				speNodeComm.SendToSpe(task);
			}
		}
		return "Success";
	}

	@SuppressWarnings("unchecked")
	public String HandleEvent(Map<String, Object> cmd) {
		String cmd_string = (String) cmd.get("task");
		List<Object> args = (List<Object>) cmd.get("arguments");
		System.out.println("Before handling " + cmd);
		List<Integer> node_ids_to_execute = (List<Integer>) cmd.getOrDefault("node", Collections.singletonList(this.node_id));

		StringBuilder ret = new StringBuilder();
		for (int node_id_to_execute : node_ids_to_execute) {
			if (node_id_to_execute != node_id) {
				ret.append(IssueTask(cmd, node_id_to_execute)).append(", ");
				break;
			}
			if ((boolean) cmd.getOrDefault("lock-spe", false)) {
				experimentAPI.LockExecution();
			}
			switch (cmd_string) {
				case "configure": {
					experimentAPI.Configure();
					break;
				}
				case "setTupleBatchSize": {
					int batch_size = (int) args.get(0);
					experimentAPI.SetTupleBatchSize(batch_size);
					break;
				}
				case "setIntervalBetweenTuples": {
					int interval = (int) args.get(0);
					experimentAPI.SetIntervalBetweenTuples(interval);
					break;
				}
				case "sendDsAsStream": {
					Map<String, Object> ds = (Map<String, Object>) args.get(0);
					int iterations = (int) args.get(1);
					boolean realism = (boolean) args.get(2);
					experimentAPI.SendDsAsStream(ds, iterations, realism);
					break;
				}
				case "sendDsAsVariableOnOffStream": {
					Map<String, Object> ds = (Map<String, Object>) args.get(0);
					int desired_tuples_per_second = (int) args.get(1);
					int downtime = (int) args.get(2);
					int min = (int) args.get(3);
					int max = (int) args.get(4);
					int step = (int) args.get(5);
					experimentAPI.SendDsAsVariableOnOffStream(ds, desired_tuples_per_second, downtime, min, max, step);
					break;
				}
				case "addSchemas": {
					List<Map<String, Object>> stream_schemas = (List<Map<String, Object>>) (List<?>) args;
					experimentAPI.AddSchemas(stream_schemas);
					break;
				}
				case "deployQueries": {
					Map<String, Object> query = (Map<String, Object>) args.get(0);
					experimentAPI.DeployQueries(query);
					break;
				}
				case "addNextHop": {
					List<Integer> schemaId_list = (List<Integer>) args.get(0);
					List<Integer> node_id_list = (List<Integer>) args.get(1);
					experimentAPI.AddNextHop(schemaId_list, node_id_list);
					break;
				}
				case "writeStreamToCsv": {
					int stream_id = (int) args.get(0);
					String csvFilename = (String) args.get(1);
					experimentAPI.WriteStreamToCsv(stream_id, csvFilename);
					break;
				}
				case "setNidToAddress": {
					Map<Integer, Map<String, Object>> newNodeIdToIpAndPort = (Map<Integer, Map<String, Object>>) args.get(0);
					experimentAPI.SetNidToAddress(newNodeIdToIpAndPort);
					break;
				}
				case "clearQueries": {
					experimentAPI.ClearQueries();
					break;
				}
				case "startRuntimeEnv": {
					experimentAPI.StartRuntimeEnv();
					break;
				}
				case "stopRuntimeEnv": {
					experimentAPI.StopRuntimeEnv();
					break;
				}
				case "endExperiment": {
					experimentAPI.EndExperiment();
					ShutDown();
					break;
				}
				case "addTpIds": {
					experimentAPI.AddTpIds(args);
					break;
				}
				case "retEndOfStream": {
					String msSinceLastReceivedTuple = experimentAPI.RetEndOfStream((int) args.get(0));
					ret.append(msSinceLastReceivedTuple).append(", ");
					break;
				}
				case "retReceivedXTuples": {
					String number_tuples = experimentAPI.RetReceivedXTuples((int) args.get(0));
					ret.append(number_tuples).append(", ");
					break;
				}
				case "wait": {
					int milliseconds = (int) args.get(0);
					experimentAPI.Wait(milliseconds);
					break;
				}
				case "traceTuple": {
					experimentAPI.TraceTuple((int) args.get(0), (List<String>) args.get(1));
					break;
				}
				case "loadSavepoint": {
					experimentAPI.LoadSavepoint((String) args.get(0));
					break;
				}
				case "loopTasks": {
					int number_iterations = (int) args.get(0);
					List<Map<String, Object>> tasks = (List<Map<String, Object>>) args.get(1);
					//experimentAPI.LoopTasks((int) args.get(0), cmds);
					for (int i = 0; i < number_iterations; i++) {
						for (Map<String, Object> inner_task : tasks) {
							HandleEvent(inner_task);
						}
					}
					break;
				}
				case "batchTasks": {
					List<Map<String, Object>> tasks = (List<Map<String, Object>>) args.get(0);
					//experimentAPI.LoopTasks((int) args.get(0), cmds);
					for (Map<String, Object> inner_task : tasks) {
						HandleEvent(inner_task);
					}
					break;
				}
				case "moveQueryState": {
					int new_host = (int) args.get(0);
					experimentAPI.MoveQueryState(new_host);
					break;
				}
				case "moveStaticQueryState": {
					int query_id = (int) args.get(0);
					int new_host = (int) args.get(1);
					experimentAPI.MoveStaticQueryState(query_id, new_host);
					break;
				}
				case "moveDynamicQueryState": {
					int query_id = (int) args.get(0);
					int new_host = (int) args.get(1);
					experimentAPI.MoveDynamicQueryState(query_id, new_host);
					break;
				}
				case "resumeStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					experimentAPI.ResumeStream(stream_id_list);
					break;
				}
				case "stopStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					int migration_coordinator_node_id = (int) args.get(1);
					experimentAPI.StopStream(stream_id_list, migration_coordinator_node_id);
					break;
				}
				case "waitForStoppedStreams": {
					List<Integer> stopping_node_id_list = (List<Integer>) args.get(0);
					List<Integer> stream_id_list = (List<Integer>) args.get(1);
					experimentAPI.WaitForStoppedStreams(stopping_node_id_list, stream_id_list);
					break;
				}
				case "bufferStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					experimentAPI.BufferStream(stream_id_list);
					break;
				}
				/*case "bufferAndStopStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					experimentAPI.BufferAndStopStream(stream_id_list);
					break;
				}
				case "bufferStopAndRelayStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					List<Integer> old_host_list = (List<Integer>) args.get(1);
					List<Integer> new_host_list = (List<Integer>) args.get(2);
					experimentAPI.BufferStopAndRelayStream(stream_id_list, old_host_list, new_host_list);
					break;
				}*/
				case "relayStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					List<Integer> old_host_list = (List<Integer>) args.get(1);
					List<Integer> new_host_list = (List<Integer>) args.get(2);
					experimentAPI.RelayStream(stream_id_list, old_host_list, new_host_list);
					break;
				}
				case "removeNextHop": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					List<Integer> host_list = (List<Integer>) args.get(1);
					experimentAPI.RemoveNextHop(stream_id_list, host_list);
					break;
				}
				case "addSourceNodes": {
					int query_id = (int) args.get(0);
					List<Integer> stream_id_list = (List<Integer>) args.get(1);
					List<Integer> node_id_list = (List<Integer>) args.get(2);
					experimentAPI.AddSourceNodes(query_id, stream_id_list, node_id_list);
					break;
				}
				case "setAsPotentialHost": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					experimentAPI.SetAsPotentialHost(stream_id_list);
					break;
				}
				case "collectMetrics": {
					int metrics_window = (int) args.get(0);
					synchronized (yaml) {
						ret.append(StringEscapeUtils.escapeJava(yaml.dump(experimentAPI.CollectMetrics(metrics_window))));
					}
					System.out.println("After handling " + cmd);
					return ret.append("\n").toString();
				}
				case "setTuplesPerSecondLimit": {
					int tuples_per_second = (int) args.get(0);
					List<Integer> node_id_list = (List<Integer>) args.get(1);
					experimentAPI.SetTuplesPerSecondLimit(tuples_per_second, node_id_list);
					break;
				}
				default: {
					this.speSpecificAPI.HandleSpeSpecificTask(cmd);
				}
			}
			if ((boolean) cmd.getOrDefault("lock-spe", false)) {
				experimentAPI.UnlockExecution();
			}
			System.out.println("After handling " + cmd);
			ret.append("Spe node ").append(node_id).append(" completed task ").append(cmd.get("task"));
		}
		return ret.toString();
	}
}
