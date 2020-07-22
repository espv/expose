package no.uio.ifi;

import org.apache.commons.cli.*;

import java.io.*;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SpeComm extends Comm {
	private String client_ip;
	int client_port;
	int node_id;
	String coordinator_ip;
	int coordinator_port;
	int spe_coordinator_port;
	ExperimentAPI experimentAPI;
	SpeSpecificAPI speSpecificAPI;
	boolean isRunning = true;
	String trace_folder;
	public CoordinatorComm speCoordinatorComm;

	private InputStreamReader fromCoordinator;
	private Map<Integer, InputStreamReader> fromSpeCoordinators = new HashMap<>();
	private Map<Integer, BufferedReader> fromSpeCoordinatorsBR = new HashMap<>();
	private BufferedReader bufferedFromCoordinator;
	private Map<Integer, BufferedReader> bufferedFromSpeCoordinators = new HashMap<>();
	private DataOutputStream outToCoordinator;
	private Map<Integer, DataOutputStream> outToSpeCoordinators = new HashMap<>();
	private Map<Integer, PrintWriter> outToSpeCoordinatorsPW = new HashMap<>();
	private Socket coordinatorSocket;
	private Map<Integer, Socket> speCoordinatorSockets = new HashMap<>();

	public SpeComm(String[] args, ExperimentAPI experimentAPI, SpeSpecificAPI speSpecificAPI) {
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

		Option c = new Option("c", "connector", true, "which connector to use for communication between nodes");
		c.setRequired(false);
		options.addOption(c);

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
		this.trace_folder = cmd.getOptionValue("trace-output-folder");
		this.experimentAPI = experimentAPI;
		this.speSpecificAPI = speSpecificAPI;
		try {
			ConnectToCoordinator(coordinator_ip, coordinator_port);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(10);
		}

		speCoordinatorComm = new CoordinatorComm(this.spe_coordinator_port, null, this.node_id);
		//speCoordinatorComm.nodeIdsToExperimentAPIs.put(this.node_id, experimentAPI);
		new Thread(speCoordinatorComm).start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void ConnectToCoordinator(String coordinator_ip, int coordinator_port) throws Exception {
		System.out.println("Connecting to coordinator at ip " + coordinator_ip + ":" + coordinator_port);
		this.coordinatorSocket = new Socket(coordinator_ip, coordinator_port);
		this.fromCoordinator = new InputStreamReader(coordinatorSocket.getInputStream());
		this.bufferedFromCoordinator = new BufferedReader(this.fromCoordinator);
		this.outToCoordinator = new DataOutputStream(coordinatorSocket.getOutputStream());
		this.outToCoordinator.writeBytes(this.node_id           + "\n" +
				this.client_ip          + "\n" +
				this.client_port + "\n" +
				this.spe_coordinator_port + "\n");
		this.outToCoordinator.flush();
	}

	public void ConnectToSpeCoordinator(int spe_coordinator_node_id, String coordinator_ip, int coordinator_port) throws Exception {
		System.out.println("Connecting to SPE coordinator at ip " + coordinator_ip + ":" + coordinator_port);
		/*this.speCoordinatorSockets.put(node_id, new Socket(coordinator_ip, coordinator_port));
		this.fromSpeCoordinators.put(node_id, new InputStreamReader(coordinatorSocket.getInputStream()));
		//this.fromSpeCoordinatorsBR.put(node_id, new BufferedReader(new InputStreamReader(coordinatorSocket.getInputStream())));
		this.bufferedFromSpeCoordinators.put(node_id, new BufferedReader(this.fromCoordinator));
		this.outToSpeCoordinators.put(node_id, new DataOutputStream(coordinatorSocket.getOutputStream()));
		//this.outToSpeCoordinatorsPW.put(node_id, new PrintWriter(coordinatorSocket.getOutputStream()));
		outToSpeCoordinators.get(node_id).writeBytes(this.node_id           + "\n" +
				this.client_ip          + "\n" +
				this.client_port + "\n" +
				this.spe_coordinator_port + "\n");
		outToSpeCoordinators.get(node_id).flush();*/


		this.speCoordinatorSockets.put(spe_coordinator_node_id, new Socket(coordinator_ip, coordinator_port));
		this.fromSpeCoordinators.put(spe_coordinator_node_id, new InputStreamReader(this.speCoordinatorSockets.get(spe_coordinator_node_id).getInputStream()));
		this.bufferedFromSpeCoordinators.put(spe_coordinator_node_id, new BufferedReader(this.fromSpeCoordinators.get(spe_coordinator_node_id)));
		this.outToSpeCoordinators.put(spe_coordinator_node_id, new DataOutputStream(speCoordinatorSockets.get(spe_coordinator_node_id).getOutputStream()));
		this.outToSpeCoordinators.get(spe_coordinator_node_id).writeBytes(this.node_id           + "\n" +
				this.client_ip          + "\n" +
				this.client_port + "\n" +
				this.spe_coordinator_port + "\n");
		this.outToSpeCoordinators.get(spe_coordinator_node_id).flush();

		// TODO: Add listener for this SPE node, on the same port as well
		new Thread(() -> {
			while (this.isRunning) {
				Map<String, Object> cmd;
				try {
					cmd = receiveMap(bufferedFromSpeCoordinators.get(spe_coordinator_node_id), yaml);
				} catch (Exception e) {
					e.printStackTrace();
					ShutDown();
					return;
				}
				String response = this.HandleEvent(cmd);
				try {
					this.outToSpeCoordinators.get(spe_coordinator_node_id).writeBytes(response);
				} catch (Exception e) {
					e.printStackTrace();
					ShutDown();
					return;
				}
			}
		}).start();
	}

	public String GetTraceOutputFolder() {return this.trace_folder;}

	public void ShutDown() {
		try {
			this.bufferedFromCoordinator.close();
			this.fromCoordinator.close();
			this.outToCoordinator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	public void AcceptTasks() {
		while (this.isRunning) {
			Map<String, Object> cmd;
			try {
				cmd = receiveMap(bufferedFromCoordinator, yaml);
			} catch (Exception e) {
				e.printStackTrace();
				ShutDown();
				return;
			}
			String response = this.HandleEvent(cmd);
			try {
				this.outToCoordinator.writeBytes(response);
			} catch (Exception e) {
				e.printStackTrace();
				ShutDown();
				return;
			}
		}
	}

	public int GetNodeId() {return this.node_id;}

	public int GetClientPort() {return this.client_port;}

	public String IssueTask(Map<String, Object> task, int node_id_to_execute) {
		// TODO: Send task to node_id_to_execute
		// TODO: For that, we must have a connection to the other nodes
		System.out.println("Issuing task " + task  + " to Node " + node_id_to_execute);
		speCoordinatorComm.SendToSpe(task);
		/*try {
			SendMap(cmd.event, this.outToSpeCoordinatorsPW.get(node_id_to_execute));
			boolean expectAck = (boolean) cmd.event.getOrDefault("ack", true);
			String ret = null;
			if (expectAck) {
				ret = fromSpeCoordinatorsBR.get(node_id_to_execute).readLine();
			}
			return ret;
		} catch (IOException e) {
			ShutDown();
			return e.toString();
		}*/
		return "Success";
	}

	@SuppressWarnings("unchecked")
	public String HandleEvent(Map<String, Object> cmd) {
		String cmd_string = (String) cmd.get("task");
		List<Object> args = (List<Object>) cmd.get("arguments");
		System.out.println("Before handling " + cmd);
		List<Integer> node_ids_to_execute = (List<Integer>) cmd.getOrDefault("node", Collections.singletonList(this.node_id));

		experimentAPI.LockExecution();
		StringBuilder ret = new StringBuilder();
		for (int node_id_to_execute : node_ids_to_execute) {
			if (node_id_to_execute != node_id) {
				return IssueTask(cmd, node_id_to_execute);
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
					experimentAPI.SendDsAsStream(ds);
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
					return msSinceLastReceivedTuple + "\n";
				}
				case "traceTuple": {
					experimentAPI.TraceTuple((int) args.get(0), (List<String>) args.get(1));
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
					int query_id = (int) args.get(0);
					int new_host = (int) args.get(1);
					experimentAPI.MoveQueryState(query_id, new_host);
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
					experimentAPI.StopStream(stream_id_list);
					break;
				}
				case "bufferStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					experimentAPI.BufferStream(stream_id_list);
					break;
				}
				case "bufferAndStopStream": {
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
				}
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
				default: {
					this.speSpecificAPI.HandleSpeSpecificTask(cmd);
				}
			}
			System.out.println("After handling " + cmd);
			ret.append("Spe node ").append(node_id).append(" completed task ").append(cmd.get("task")).append("\n");
		}
		experimentAPI.UnlockExecution();
		return ret.toString();
	}
}
