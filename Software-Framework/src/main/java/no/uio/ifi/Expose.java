package no.uio.ifi;

import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

public class Expose {
	private Map<String, Object> yaml_configuration;
	private TracingFramework tf;
	private boolean isCoordinator;
	private boolean available = true;
	private CoordinatorComm mc;
	private ExperimentAPI experimentAPI;

	private String trace_output_folder;

	Expose(String yaml_file, String trace_output_folder, boolean isCoordinator) {
		this.yaml_configuration = Loadyaml(yaml_file);
		this.trace_output_folder = trace_output_folder;
		this.isCoordinator = isCoordinator;
		this.tf = new TracingFramework();
	}

	void setExperimentAPI(ExperimentAPI experimentAPI) {
		this.experimentAPI = experimentAPI;
	}

	class TaskHandler implements Runnable {
		String cmd;
		List<Object> args;
		int node_id;

		TaskHandler(String cmd, List<Object> args, int node_id) {
			this.cmd = cmd;
			this.args = args;
			this.node_id = node_id;
		}

		public void doHandleEvent() {
			String ret = null;
			switch (cmd) {
				case "setEventBatchSize": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).SetTupleBatchSize((int) args.get(0));
					break;
				}
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
				case "runEnvironment": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).RunEnvironment();
					break;
				}
				case "stopEnvironment": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).StopEnvironment();
					break;
				}
				case "setIntervalBetweenEvents": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).SetIntervalBetweenTuples((int) args.get(0));
					break;
				}
				case "addEvents": {
					System.out.println("Adding events with arguments " + args);
					int event_id = (int) args.get(0);
					Map<String, Object> spe_event_to_create = null;
					List<Map<String, Object>>
							speevents = (ArrayList<Map<String, Object>>) yaml_configuration.get("speevents");
					for (Map<String, Object> speevent : speevents) {
						if ((int) speevent.get("id") == event_id) {
							spe_event_to_create = speevent;
							break;
						}
					}

					ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddTuples(spe_event_to_create, (int) args.get(1));
					break;
				}
				case "addDataset": {
					int dataset_id = (int) args.get(0);
					List<Map<String, Object>>
							datasets = (ArrayList<Map<String, Object>>) yaml_configuration.get("datasets");
					for (Map<String, Object> ds : datasets) {
						if ((int) ds.get("id") == dataset_id) {
							ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddDataset(ds);
							break;
						}
					}
					break;
				}
				case "addQueries": {
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
					for (int i = 0; i < quantity; ++i) {
						ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddQueries(spe_rule_to_create);
					}
					break;
				}
				case "loopTasks": {
					int numberIterations = (int) args.get(0);
					for (int i = 0; i < numberIterations; i++) {
						List<Map<String, Object>> cmds = (ArrayList<Map<String, Object>>) args.get(1);
						for (Map<String, Object> inner_cmd : cmds) {
							handleEvent(inner_cmd);
						}
					}
					break;
				} case "addSubscriberOfStream": {
					List<Map<String, Object> >
							streamDefinitions = (ArrayList<Map<String, Object> >) yaml_configuration.get("stream-definitions");
					for (Map<String, Object> stream_definition: streamDefinitions) {
						if ((int) stream_definition.get("id") == (int) args.get(0)) {
							ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddSubscriberOfStream((int) stream_definition.get("stream-id"), (int) args.get(1));
						}
					}
					break;
				} case "processEvents": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).ProcessTuples((int) args.get(0));
					break;
				}
				case "processDataset": {
					int dataset_id = (int) args.get(0);
					List<Map<String, Object>>
							datasets = (ArrayList<Map<String, Object>>) yaml_configuration.get("datasets");
					for (Map<String, Object> ds : datasets) {
						ds.put("file", ds.get("file"));
						if ((int) ds.get("id") == dataset_id) {
							ret = mc.nodeIdsToExperimentAPIs.get(node_id).ProcessDataset(ds);
							break;
						}
					}
					break;
				}
				case "runParallel": {
					for (Object o : args) {
						Map<String, Object> inner_cmd = (Map<String, Object>) o;
						handleEvent(inner_cmd);
					}
					// Wait for ack here
					for (Object o : args) {
						Map<String, Object> inner_cmd = (Map<String, Object>) o;
						int nodeId = (int) inner_cmd.get("node");
						mc.PrintIncomingOfClient(nodeId);
					}
					break;
				}
				case "clearQueries": {
					tf.traceEvent(222);
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).ClearQueries();
					break;
				}
				case "clearEvents": {
					tf.traceEvent(223);
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).ClearTuples();
					break;
				}
				case "setNodeIdToAddress": {
					Map<Integer, Map<String, Object> > nodeIdToIpAndPort = (Map<Integer, Map<String, Object> >) args.get(0);
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).SetNodeIdToAddress(nodeIdToIpAndPort);
					break;
				}
				case "cleanupExperiment": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).CleanupExperiment();
					break;
				}
				case "addTracepointIds": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).AddTracepointIds(args);
					break;
				}
				case "notifyAfterNoReceivedTuple": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).NotifyAfterNoReceivedTuple((int) args.get(0));
					System.out.println("Last received tuple: " + ret + " ms ago");
					break;
				}
				case "traceTuple": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).TraceTuple((int) args.get(0), (List<String>) args.get(1));
					break;
				}
				case "configure": {
					ret = mc.nodeIdsToExperimentAPIs.get(node_id).Configure();
					break;
				}
				default: {
					throw new RuntimeException("Unknown task " + cmd + " parsed");
				}
			}
		}

		public void run() {
			doHandleEvent();
		}
	}

	@SuppressWarnings("unchecked")
	void handleEvent(Map<String, Object> event) {
		String cmd = (String)event.get("task");
		List<Object> args = (List<Object>) event.get("arguments");
		int node_id = 0;
		if (!event.get("node").equals("coordinator")) {
			node_id = (int) event.get("node");
		}
		System.out.println("Node " + node_id + ": " + event);
		TaskHandler taskHandler = new TaskHandler(cmd, args, node_id);
		if ((boolean) event.getOrDefault("parallel", false)) {
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
			tf.addTracepoint(tracepoint_id);
			activeTracepointIds.add(tracepoint_id);
		}
		System.out.println("Node " + node_id + ": AddTracepointIds " + activeTracepointIds);
		this.mc.nodeIdsToExperimentAPIs.get(node_id).AddTracepointIds(activeTracepointIds);

		List<Map<String, Object> >
				streamDefinitions = (ArrayList<Map<String, Object> >) yaml_configuration.get("stream-definitions");
		this.mc.nodeIdsToExperimentAPIs.get(node_id).AddStreamSchemas(streamDefinitions);
		System.out.println("Node " + node_id + ": AddStreamSchemas " + streamDefinitions);
	}

	void WaitForSPEs(List<Map<String, Object>> cmds, Map<Integer, CoordinatorComm.CoordinatorClient> nodeIdsToClients) {
		for (Map<String, Object> event : cmds) {
			boolean isCoordinator = event.get("node").equals("coordinator");
			if (event.get("task").equals("loopTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> loopCmds = (ArrayList<Map<String, Object>>) args.get(1);
				WaitForSPEs(loopCmds, nodeIdsToClients);
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

	@SuppressWarnings("unchecked")
	void InterpretEvents(int eid) {
		System.out.println("Running from yaml config");
		List<Map<String, Object> >
			experiments = (ArrayList<Map<String, Object> >) yaml_configuration.get("experiments");
		for (Map<String, Object> experiment: experiments) {
			if (experiment.get("id").equals(eid)) {
				List<Map<String, Object> > cmds = (ArrayList<Map<String, Object> >) experiment.get("flow");
				tf.traceEvent(0, new Object[]{eid});
				Map<Integer, CoordinatorComm.CoordinatorClient> nodeIdsToClients = mc.GetNodeIdsToClients();
				// Pre-processing to ensure that all the necessary mediators are registered
				WaitForSPEs(cmds, nodeIdsToClients);

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// All agents have registered, and now they can set up connections
				Map<String, Object> cmd = new HashMap<>();
				cmd.put("task", "Configure");
				this.mc.SendCoordinatorTask(cmd);

				for (Map<String, Object> event : cmds) {
					while (!SetUnavailableIfAvailable()) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					handleEvent(event);
					SetAvailable();
				}
				break;
			}
		}

		this.mc.CleanupExperimentTask();
	}


	void WriteBufferToFile(String prefix_fn) {
		tf.writeTraceToFile(trace_output_folder, prefix_fn);
	}

	Map<String, Object> Loadyaml(String yaml_file) {
		FileInputStream fis = null;
		Yaml yaml = new Yaml();
		try {
			fis = new FileInputStream(yaml_file.replaceFirst("^~", System.getProperty("user.home")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
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

		Option output = new Option("t", "trace-folder", true, "trace output folder");
		output.setRequired(true);
		options.addOption(output);

		Option coordinator = new Option("m", "is-coordinator", false, "Is coordinator");
		coordinator.setRequired(true);
		options.addOption(coordinator);

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
		String traceFileFolder = cmd.getOptionValue("trace-folder");
		boolean isCoordinator = cmd.hasOption("is-coordinator");


		Expose jspef = new Expose(
			configFilePath, traceFileFolder, isCoordinator);

		Thread t = new Thread(new Runnable() {
			boolean keepRunning = true;

			void handleEvent(Map<String, Object> map) {
				try {
					jspef.handleEvent(map);
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
				System.out.println("Example: {task: addQueries, arguments: [10, 1], node: 1");
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				while (keepRunning) {
					Yaml yaml = new Yaml();
					Map<String, Object> map;
					try {
						map = yaml.load(reader.readLine());
					} catch (ClassCastException e) {
						e.printStackTrace();
						System.err.println("Task must be formatted as a JSON object");
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
					// An example use case is to call stopEnvironment in flink to stop the flink job
					if (clearToExecute || alwaysRun) {
						System.out.println("Executing user-task " + map);
						if (!requireAck) {
							// This might be the task runEnvironment in flink which starts a flink job
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

		jspef.mc = new CoordinatorComm(Integer.parseInt(cmd.getOptionValue("coordinator-port")), jspef);
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
