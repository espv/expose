package no.uio.ifi;

import org.apache.commons.cli.*;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;


public class SpeComm extends Comm {
	private String client_ip;
	int client_port;
	int node_id;
	String coordinator_ip;
	int coordinator_port;
	ExperimentAPI experimentAPI;
	boolean isRunning = true;
	String trace_folder;

	private InputStreamReader fromCoordinator;
	private BufferedReader bufferedFromCoordinator;
	private DataOutputStream outToCoordinator;
	private Socket coordinatorSocket;

	public SpeComm(String[] args, ExperimentAPI experimentAPI) {
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
		this.trace_folder = cmd.getOptionValue("trace-output-folder");
		this.experimentAPI = experimentAPI;
		try {
			ConnectToCoordinator(coordinator_ip, coordinator_port);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(10);
		}
	}

	public void ConnectToCoordinator(String coordinatorIp, int coordinatorPort) throws Exception {
		System.out.println("Connecting to coordinator at ip " + coordinatorIp + ":" + coordinatorPort);
		this.coordinatorSocket = new Socket(coordinatorIp, coordinatorPort);
		this.fromCoordinator = new InputStreamReader(coordinatorSocket.getInputStream());
		this.bufferedFromCoordinator = new BufferedReader(this.fromCoordinator);
		this.outToCoordinator = new DataOutputStream(coordinatorSocket.getOutputStream());
		outToCoordinator.writeBytes(node_id           + "\n" +
				this.client_ip          + "\n" +
				this.client_port + "\n");
		outToCoordinator.flush();
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
			Task cmd;
			try {
				Map<String, Object> event = receiveMap(bufferedFromCoordinator, yaml);
				cmd = new Task(event);
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

	@SuppressWarnings("unchecked")
	public String HandleEvent(Task cmd) {
		String cmd_string = (String) cmd.event.get("task");
		List<Object> args = (List<Object>) cmd.event.get("arguments");
		System.out.println("Before handling " + cmd.event);
		switch (cmd_string) {
			case "Configure": {
				experimentAPI.Configure();
				break;
			} case "SetTupleBatchSize": {
				int batch_size = (int) args.get(0);
				experimentAPI.SetTupleBatchSize(batch_size);
				break;
			} case "SetIntervalBetweenTuples": {
				int interval = (int) args.get(0);
				experimentAPI.SetIntervalBetweenTuples(interval);
				break;
			} case "AddTuples": {
				Map<String, Object> tuple = (Map<String, Object>) args.get(0);
				int quantity = (int) args.get(1);
				experimentAPI.AddTuples(tuple, quantity);
				break;
			} case "AddDataset": {
				Map<String, Object> ds = (Map<String, Object>) args.get(0);
				experimentAPI.AddDataset(ds);
				break;
			} case "SendDsAsStream": {
				Map<String, Object> ds = (Map<String, Object>) args.get(0);
				experimentAPI.SendDsAsStream(ds);
				break;
			} case "AddSchemas": {
				List<Map<String, Object>> stream_schemas = (List<Map<String, Object>>) (List<?>) args;
				experimentAPI.AddSchemas(stream_schemas);
				break;
			} case "DeployQueries": {
				Map<String, Object> query = (Map<String, Object>) args.get(0);
				experimentAPI.DeployQueries(query);
				break;
			} case "AddNextHop": {
				int schemaId = (int) args.get(0);
				int node_id = (int) args.get(1);
				experimentAPI.AddNextHop(schemaId, node_id);
				break;
			} case "WriteStreamToCsv": {
				int stream_id = (int) args.get(0);
				String csvFilename = (String) args.get(1);
				experimentAPI.WriteStreamToCsv(stream_id, csvFilename);
				break;
			} case "SetNidToAddress": {
				Map<Integer, Map<String, Object> > newNodeIdToIpAndPort = (Map<Integer, Map<String, Object>>) args.get(0);
				experimentAPI.SetNidToAddress(newNodeIdToIpAndPort);
				break;
			} case "ProcessTuples": {
				int number_tuples = (int) args.get(0);
				experimentAPI.ProcessTuples(number_tuples);
				break;
			} case "ClearQueries": {
				experimentAPI.ClearQueries();
				break;
			} case "ClearTuples": {
				experimentAPI.ClearTuples();
				break;
			} case "StartRuntimeEnv": {
				experimentAPI.StartRuntimeEnv();
				break;
			} case "StopRuntimeEnv": {
				experimentAPI.StopRuntimeEnv();
				break;
			} case "EndExperiment": {
				experimentAPI.EndExperiment();
				ShutDown();
				break;
			} case "AddTpIds": {
				experimentAPI.AddTpIds(args);
				break;
			} case "RetEndOfStream": {
				String msSinceLastReceivedTuple = experimentAPI.RetEndOfStream((int) args.get(0));
				return msSinceLastReceivedTuple + "\n";
			} case "TraceTuple": {
				experimentAPI.TraceTuple((int) args.get(0), (List<String>) args.get(1));
				break;
			} default: {
				throw new RuntimeException("Invalid task from mediator: " + cmd_string);
			}
		}
		System.out.println("After handling " + cmd.event);
		return "Spe node " + node_id + " completed task " + cmd.event.get("task") + "\n";
	}
}
