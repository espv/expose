package no.uio.ifi;

import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

public class Expose {
	private Map<String, Object> yaml_configuration;
	private boolean available = true;
	private CoordinatorComm mc;
	List<Map<String, Object>> tasks;

	private int cmd_number = 1;

	Expose(String yaml_file) {
		this.yaml_configuration = Loadyaml(yaml_file);
	}

	class TaskHandler implements Runnable {
		Map<String, Object> task;

		TaskHandler(Map<String, Object> task) {
			this.task = task;
		}

		public void doHandleEvent() {
			int node_id = (int) task.get("node");
			if (node_id != 0) {
				mc.SendToSpe(task);
				return;
			}
			String cmd = (String) task.get("task");
			List<Object> args = (List<Object>) task.get("arguments");
			switch (cmd) {
				case "wait": {
					if (args.size() == 0) {
						System.out.println("Hit enter when you want to continue to next task");
						BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
						try {
							reader.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
						System.out.println("Wait is done");
					} else {
						try {
							Thread.sleep((int) args.get(0));
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					break;
				}
				case "loopTasks": {
					int numberIterations = (int) args.get(0);
					List<Map<String, Object>> tasks = (List<Map<String, Object>>) args.get(1);
					for (int i = 0; i < numberIterations; i++) {
						for (Map<String, Object> inner_task : tasks) {
							handleEvent(inner_task);
						}
					}
					//ret = mc.nodeIdsToExperimentAPIs.get(node_id).LoopTasks(numberIterations, cmds);
					break;
				}
				case "batchTasks": {
					List<Map<String, Object>> tasks = (List<Map<String, Object>>) args.get(0);
					for (Map<String, Object> inner_task : tasks) {
						handleEvent(inner_task);
					}
					//ret = mc.nodeIdsToExperimentAPIs.get(node_id).LoopTasks(numberIterations, cmds);
					break;
				}
				/*case "migrateQueryState": {
					ret = nodeIdsToExperimentAPIs.get(node_id).MoveQueryState(query_id, old_host, new_host);
					break;
				}
				case "migrateStaticQueryState": {
					ret = nodeIdsToExperimentAPIs.get(node_id).MoveStaticQueryState(query_id, old_host, new_host);
					break;
				}
				case "migrateDynamicQueryState": {
					ret = nodeIdsToExperimentAPIs.get(node_id).MoveDynamicQueryState(query_id, old_host, new_host);
					break;
				}
				case "resumeQuery": {
					ret = nodeIdsToExperimentAPIs.get(node_id).ResumeQuery(query_id);
					break;
				}
				case "stopQuery": {
					ret = nodeIdsToExperimentAPIs.get(node_id).StopQuery(query_id);
					break;
				}
				case "bufferQuery": {
					ret = nodeIdsToExperimentAPIs.get(node_id).BufferQuery(query_id);
					break;
				}
				case "stopAndBufferQuery": {
					ret = nodeIdsToExperimentAPIs.get(node_id).StopAndBufferQuery(query_id);
					break;
				}
				case "relayStream": {
					ret = nodeIdsToExperimentAPIs.get(node_id).ResumeStream(stream_id, old_host, new_host);
					break;
				}
				case "removeNextHop": {
					ret = nodeIdsToExperimentAPIs.get(node_id).RemoveNextHop(stream_id, old_host, new_host);
					break;
				}
				case "performQueryMigration": {
					ret = nodeIdsToExperimentAPIs.get(node_id).PerformQueryMigration(query_id, old_host, new_host, migration_policy);
					break;
				}
				case "deploySubQueries": {
					// If the query with query_id gets migrated, all number_instances of them will get migrated to the
					// new host, and the source_nodes will be notified so that they can transmit tuples to the new host
					ret = nodeIdsToExperimentAPIs.get(node_id).DeploySubQueries(query_id, number_instances, source_nodes);
					break;
				}*/
				default: {
					throw new RuntimeException("Unknown task " + task.get("task") + " parsed");
				}
			}
		}

		public void run() {
			doHandleEvent();
		}
	}

	@SuppressWarnings("unchecked")
	void handleEvent(Map<String, Object> task) {
		++cmd_number;
		int node_id = (int) task.get("node");
		System.out.println("Line " + cmd_number + ": Node " + node_id + " executes " + task);
		TaskHandler taskHandler = new TaskHandler(task);
		// TODO: add whether the task should be executed in parallel
		if ((boolean) task.getOrDefault("parallel", false)) {
			new Thread(taskHandler).start();
		} else {
			taskHandler.doHandleEvent();
		}
	}

	void Configure(int node_id) {
		List<Map<String, Object> >
				tracepoints = (ArrayList<Map<String, Object> >) yaml_configuration.get("tracepoints");
		List<Object> activeTracepointIds = new ArrayList<>();
		for (Map<String, Object> tracepoint : tracepoints) {
			if (!(boolean)tracepoint.get("active")) {
				continue;
			}
			int tracepoint_id = (int)tracepoint.get("id");
			activeTracepointIds.add(tracepoint_id);
		}
		System.out.println("Node " + node_id + ": AddTpIds " + activeTracepointIds);
		Map<String, Object> map = new HashMap<>();
		map.put("task", "addTpIds");
		map.put("arguments", activeTracepointIds);
		map.put("node", node_id);
		mc.SendToSpe(map);

		//this.mc.nodeIdsToExperimentAPIs.get(node_id).AddTpIds(activeTracepointIds);

		List<Map<String, Object> >
				streamDefinitions = (ArrayList<Map<String, Object> >) yaml_configuration.get("stream-definitions");

		map = new HashMap<>();
		map.put("task", "addSchemas");
		map.put("arguments", streamDefinitions);
		map.put("node", node_id);
		mc.SendToSpe(map);
		//this.mc.nodeIdsToExperimentAPIs.get(node_id).AddSchemas(streamDefinitions);
		System.out.println("Node " + node_id + ": AddSchemas " + streamDefinitions);
	}

	void WaitForSPEs(List<Map<String, Object>> cmds, Map<Integer, CoordinatorComm.CoordinatorClient> nodeIdsToClients) {
		for (Map<String, Object> event : cmds) {
			boolean isCoordinator = event.get("node").equals("coordinator");
			if (event.get("task").equals("loopTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> loopCmds = (ArrayList<Map<String, Object>>) args.get(1);
				WaitForSPEs(loopCmds, nodeIdsToClients);
			} else if (event.get("task").equals("batchTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> batchCmds = (ArrayList<Map<String, Object>>) args.get(0);
				WaitForSPEs(batchCmds, nodeIdsToClients);
			}
			if (isCoordinator) {
				continue;
			}
			int node_id = (int) event.get("node");
			while (!this.mc.nodeIdReady.getOrDefault(node_id, false)) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(5);
				}
			}
		}
	}

	Map<String, Object> PreprocessTask(Map<String, Object> raw_task) {
		String cmd = (String) raw_task.get("task");
		int node_id = 0;
		if (!raw_task.get("node").equals("coordinator")) {
			node_id = (int) raw_task.get("node");
		}
		List<Object> args = (List<Object>) raw_task.get("arguments");

		List<Object> task_args = new ArrayList<>();

		Map<String, Object> map = new HashMap<>();
		for (String k : raw_task.keySet()) {
			map.put(k, raw_task.get(k));
		}
		map.put("node", node_id);

		switch (cmd) {
			case "setEventBatchSize": {
				task_args.add(args.get(0));
				break;
			}
			case "wait": {
				/*if (args.size() == 0) {
					System.out.println("Hit enter when you want to continue to next task");
					BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
					try {
						reader.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					System.out.println("Wait is done");
				} else {
					try {
						Thread.sleep((int) args.get(0));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				break;*/
			}
			case "startRuntimeEnv": {
				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).StartRuntimeEnv();
				break;
			}
			case "stopRuntimeEnv": {
				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).StopRuntimeEnv();
				break;
			}
			case "setIntervalBetweenEvents": {
				task_args.add(args.get(0));
				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).SetIntervalBetweenTuples((int) args.get(0));
				break;
			}
			case "deployQueries": {
				int query_id = (int) args.get(0);
				Map<String, Object> spe_rule_to_create = null;
				List<Map<String, Object>>
						spequeries = (ArrayList<Map<String, Object>>) yaml_configuration.get("spequeries");
				for (Map<String, Object> spequery : spequeries) {
					if ((int) spequery.get("id") == query_id) {
						spe_rule_to_create = spequery;
						break;
					}
				}

				int quantity = (int) args.get(1);
				task_args.add(spe_rule_to_create);
				for (int i = 0; i < quantity; ++i) {
					// TODO: Add number of iterations as argument
					//ret = mc.nodeIdsToExperimentAPIs.get(node_id).DeployQueries(spe_rule_to_create);
				}
				break;
			}
			case "loopTasks": {
				int numberIterations = (int) args.get(0);
				List<Map<String, Object>> cmds = (List<Map<String, Object>>) args.get(1);
				List<Map<String, Object>> tasks = new ArrayList<>();
				for (Map<String, Object> inner_cmd : cmds) {
					Map<String, Object> inner_task = PreprocessTask(inner_cmd);
					tasks.add(inner_task);
				}

				task_args.add(numberIterations);
				task_args.add(tasks);
				break;
			}
			case "batchTasks": {
				List<Map<String, Object>> cmds = (List<Map<String, Object>>) args.get(0);
				List<Map<String, Object>> tasks = new ArrayList<>();
				for (Map<String, Object> inner_cmd : cmds) {
					Map<String, Object> inner_task = PreprocessTask(inner_cmd);
					tasks.add(inner_task);
				}

				task_args.add(tasks);
				break;
			}
			case "addNextHop": {
				List<Map<String, Object>>
						streamDefinitions = (ArrayList<Map<String, Object>>) yaml_configuration.get("stream-definitions");
				for (Map<String, Object> stream_definition : streamDefinitions) {
					if ((int) stream_definition.get("id") == (int) args.get(0)) {
						int streamId = (int) stream_definition.get("stream-id");
						int nodeId = (int) args.get(1);
						task_args.add(streamId);
						task_args.add(nodeId);
						//ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddNextHop((int) stream_definition.get("stream-id"), (int) args.get(1));
						break;
					}
				}
				break;
			}
			case "writeStreamToCsv": {
				int stream_id = (int) args.get(0);
				String csvFolder = (String) args.get(1);
				task_args.add(stream_id);
				task_args.add(csvFolder);
				//mc.nodeIdsToExperimentAPIs.get(node_id).WriteStreamToCsv(stream_id, csvFolder);
				break;
			}
			case "sendDsAsStream": {
				int dataset_id = (int) args.get(0);
				List<Map<String, Object>>
						datasets = (ArrayList<Map<String, Object>>) yaml_configuration.get("datasets");
				for (Map<String, Object> ds : datasets) {
					ds.put("file", ds.get("file"));
					if ((int) ds.get("id") == dataset_id) {
						task_args.add(ds);

						//ret = mc.nodeIdsToExperimentAPIs.get(node_id).SendDsAsStream(ds);
						break;
					}
				}
				break;
			}
			case "clearQueries": {

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).ClearQueries();
				break;
			}
			case "setNidToAddress": {
				Map<Integer, Map<String, Object>> nodeIdToIpAndPort = (Map<Integer, Map<String, Object>>) args.get(0);

				task_args.add(nodeIdToIpAndPort);

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).SetNidToAddress(nodeIdToIpAndPort);
				break;
			}
			case "endExperiment": {

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).EndExperiment();
				break;
			}
			case "addTpIds": {
				map.put("arguments", args);

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddTpIds(args);
				break;
			}
			case "retEndOfStream": {
				int nanoseconds = (int) args.get(0);
				task_args.add(nanoseconds);

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).RetEndOfStream((int) args.get(0));
				break;
			}
			case "traceTuple": {
				int tracepointId = (int) args.get(0);
				List<String> traceArguments = (List<String>) args.get(1);

				task_args.add(tracepointId);
				task_args.add(traceArguments);

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).TraceTuple((int) args.get(0), (List<String>) args.get(1));
				break;
			}
			case "configure": {

				//ret = mc.nodeIdsToExperimentAPIs.get(node_id).Configure();
				break;
			} case "moveQueryState": {
				int query_id = (int) args.get(0);
				int new_host = (int) args.get(1);
				task_args.add(query_id);
				task_args.add(new_host);
				break;
			} case "moveStaticQueryState": {
				int query_id = (int) args.get(0);
				int new_host = (int) args.get(1);
				task_args.add(query_id);
				task_args.add(new_host);
				break;
			} case "moveDynamicQueryState": {
				int query_id = (int) args.get(0);
				int new_host = (int) args.get(1);
				task_args.add(query_id);
				task_args.add(new_host);
				break;
			} case "resumeStream": {
				int stream_id = (int) args.get(0);
				task_args.add(stream_id);
				break;
			} case "stopStream": {
				int stream_id = (int) args.get(0);
				task_args.add(stream_id);
				break;
			} case "bufferStream": {
				int stream_id = (int) args.get(0);
				task_args.add(stream_id);
				break;
			} case "bufferAndStopStream": {
				int stream_id = (int) args.get(0);
				task_args.add(stream_id);
				break;
			} case "bufferStopAndRelayStream": {
				int stream_id = (int) args.get(0);
				int old_host = (int) args.get(1);
				int new_host = (int) args.get(2);
				task_args.add(stream_id);
				task_args.add(old_host);
				task_args.add(new_host);
				break;
			} case "relayStream": {
				int stream_id = (int) args.get(0);
				int old_host = (int) args.get(1);
				int new_host = (int) args.get(2);
				task_args.add(stream_id);
				task_args.add(old_host);
				task_args.add(new_host);
				break;
			} case "removeNextHop": {
				int stream_id = (int) args.get(0);
				int host = (int) args.get(1);
				task_args.add(stream_id);
				task_args.add(host);
				break;
			} case "addSourceNodes": {
				int query_id = (int) args.get(0);
				int stream_id = (int) args.get(1);
				List<Integer> node_id_list = (List<Integer>) args.get(2);
				task_args.add(query_id);
				task_args.add(stream_id);
				task_args.add(node_id_list);
				break;
			}
			default: {
				throw new RuntimeException("Unknown task " + cmd + " parsed");
			}
		}

		map.put("arguments", task_args);
		return map;
	}

	void PreprocessTasks(List<Map<String, Object> > raw_tasks) {
		tasks = new ArrayList<>();
		for (Map<String, Object> raw_task : raw_tasks) {
			Map<String, Object> task = PreprocessTask(raw_task);
			tasks.add(task);
		}
	}

	@SuppressWarnings("unchecked")
	void InterpretEvents(int eid) {
		System.out.println("Running from yaml config");
		List<Map<String, Object> >
			experiments = (ArrayList<Map<String, Object> >) yaml_configuration.get("experiments");
		for (Map<String, Object> experiment: experiments) {
			if (experiment.get("id").equals(eid)) {
				List<Map<String, Object> > cmds = (ArrayList<Map<String, Object> >) experiment.get("flow");
				Map<Integer, CoordinatorComm.CoordinatorClient> nodeIdsToClients = mc.GetNodeIdsToClients();
				PreprocessTasks(cmds);
				// Ensure that all the necessary nodes are registered
				WaitForSPEs(cmds, nodeIdsToClients);
				// SetNidToAddress
				Map<String, Object> cmd = new HashMap<>();
				cmd.put("task", "setNidToAddress");
				List<Map<Integer, Map<String, Object>>> args = new ArrayList<>();
				args.add(mc.nodeIdsToClientInformation);
				cmd.put("arguments", args);
				mc.SendCoordinatorTask(cmd);

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(20);
				}

				// All agents have registered, and now they can set up connections
				cmd = new HashMap<>();
				cmd.put("task", "configure");
				this.mc.SendCoordinatorTask(cmd);

				for (Map<String, Object> task : tasks) {
					while (!SetUnavailableIfAvailable()) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
							System.exit(21);
						}
					}
					handleEvent(task);
					SetAvailable();
				}
				break;
			}
		}

		this.mc.EndExperimentTask();
	}

	Map<String, Object> Loadyaml(String yaml_file) {
		FileInputStream fis = null;
		Yaml yaml = new Yaml();
		try {
			fis = new FileInputStream(yaml_file.replaceFirst("^~", System.getProperty("user.home")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(22);
		}
		Map<String, Object> map = yaml.load(fis);

		return map;
	}

	synchronized boolean SetUnavailableIfAvailable() {
		if (!available) {
			return false;
		}

		available = false;
		return true;
	}

	void SetAvailable() {available = true;}

	public static void main(String[] args) {
		Options options = new Options();

		Option input = new Option("y", "yaml-config", true, "yaml config file path");
		input.setRequired(true);
		options.addOption(input);

		Option coordinatorPort = new Option("p", "coordinator-port", true, "Port of coordinator");
		coordinatorPort.setRequired(true);
		options.addOption(coordinatorPort);

		Option experimentId = new Option("e", "experiment-id", true, "Experiment ID");
		experimentId.setRequired(true);
		options.addOption(experimentId);

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

		String configFilePath = cmd.getOptionValue("yaml-config");
		Expose jspef = new Expose(configFilePath);

		Thread t = new Thread(new Runnable() {
			boolean keepRunning = true;

			void handleEvent(Map<String, Object> map) {
				try {
					handleEvent(map);
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("User-task caused an error");
				} finally {
					jspef.SetAvailable();
				}
			}

			@Override
			public void run() {
				System.out.println("Give custom tasks in the form of a JSON object");
				System.out.println("Example: {task: deployQueries, arguments: [10, 1], node: 1");
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				while (keepRunning) {
					Yaml yaml = new Yaml();
					Map<String, Object> map;
					try {
						map = yaml.load(reader.readLine());
					} catch (ClassCastException e) {
						e.printStackTrace();
						System.err.println("Map<String, Object> must be formatted as a JSON object");
						continue;
					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}

					if (map == null) {
						// Comment or empty line come here
						continue;
					}

					// If no ack is required for the task, we can just execute it in parallel
					boolean clearToExecute = jspef.SetUnavailableIfAvailable();
					boolean alwaysRun = (boolean) map.getOrDefault("always-run", false);
					boolean requireAck = (boolean) map.getOrDefault("ack", true);
					// always-run is a special flag that will always run, regardless of what else is running
					// An example use case is to call stopRuntimeEnv in flink to stop the flink job
					if (clearToExecute || alwaysRun) {
						System.out.println("Executing user-task " + map);
						if (!requireAck) {
							// This might be the task startRuntimeEnv in flink which starts a flink job
							new Thread(() -> handleEvent(map)).start();
						} else {
							handleEvent(map);
						}
						System.out.println("Finished executing user-task");
					} else {
						System.out.println("A task is currently being executed. Try again later");
					}
				}
			}
		});

		jspef.mc = new CoordinatorComm(Integer.parseInt(cmd.getOptionValue("coordinator-port")), jspef, 0);
		new Thread(jspef.mc).start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		boolean cont = true;
		String eid = cmd.getOptionValue("experiment-id");
		while (cont) {
			if (eid.isEmpty()) {
				System.out.println("Select experiment ID");
				try {
					eid = reader.readLine();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(24);
				}
			}
			System.out.println("Starting experiment " + eid);
			jspef.InterpretEvents(Integer.parseInt(eid));
			System.out.println("Finished experiment " + eid);
			System.out.println();
			System.exit(0);
			System.out.println("Continue with new experiment? y/n");
			String response = "";
			while (!response.equals("n") && !response.equals("y")) {
				try {
					response = reader.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (response == null) {
					// Exiting program
					return;
				}
			}
			if (response.equals("n")) {
				cont = false;
			}
		}
	}
}
