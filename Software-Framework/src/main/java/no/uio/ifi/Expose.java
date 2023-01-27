package no.uio.ifi;

import com.mxgraph.layout.mxOrganicLayout;
import com.mxgraph.swing.mxGraphComponent;
import org.apache.commons.cli.*;
import org.apache.commons.text.StringEscapeUtils;
import org.jgrapht.ListenableGraph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultListenableGraph;
import org.yaml.snakeyaml.Yaml;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.List;

class ExposeGraphJApplet extends JApplet {
	public static final long serialVersionUID = 2202072534703043194L;

	public static final Dimension DEFAULT_SIZE = new Dimension(530, 320);

	public JGraphXAdapter<String, ExposeEdge> jgxAdapter;

	ExposeGraphJApplet() {

	}
}

class ExposeEdge extends DefaultEdge {
	String id;

	public ExposeEdge() {
	}

	public void setId(String id) {
		this.id = id;
	}

	public String toString() {
		return this.id;
	}
}

class GraphGenerator {
	ListenableGraph<String, ExposeEdge> g;
	ExposeGraphJApplet applet = new ExposeGraphJApplet();

	GraphGenerator() {
		this.g = new DefaultListenableGraph<>(new DefaultDirectedGraph<>(ExposeEdge.class));
		applet.init();
		// create a visualization using JGraph, via an adapter
		applet.jgxAdapter = new JGraphXAdapter<>(g);

		applet.setPreferredSize(ExposeGraphJApplet.DEFAULT_SIZE);
		mxGraphComponent component = new mxGraphComponent(applet.jgxAdapter);
		component.setConnectable(false);
		component.getGraph().setAllowDanglingEdges(false);
		applet.getContentPane().add(component);
		applet.resize(ExposeGraphJApplet.DEFAULT_SIZE);
	}

	void AddVertex(int node_id) {
		String id = Integer.toString(node_id);
		if (g.containsVertex(id)) {
			return;
		}
		g.addVertex(id);
	}

	void AddEdge(List<Integer> from_nodes, List<Integer> streams, List<Integer> to_nodes) {
		for (int from_node : from_nodes) {
			AddVertex(from_node);
			for (int to_node : to_nodes) {
				AddVertex(to_node);
				StringBuilder sb = new StringBuilder();
				for (int stream_id : streams) {
					sb.append(stream_id).append(", ");
				}

				String from = Integer.toString(from_node);
				String to = Integer.toString(to_node);
				String id = streams.toString().replaceAll(", $", "");
				if (g.containsEdge(from, to)) {
					sb.insert(0, g.getEdge(from, to).id).append(", ");
					g.getEdge(from, to).id = id;
				} else {
					ExposeEdge de = g.addEdge(from, to);
					de.setId(id);
				}
			}
		}
	}

	void RemoveEdge(List<Integer> from_nodes, List<Integer> to_nodes) {
		for (int from_node : from_nodes) {
			for (int to_node : to_nodes) {
				String from = Integer.toString(from_node);
				String to = Integer.toString(to_node);
				g.removeEdge(from, to);
			}
		}
	}

	/*void DoGenerateNetworkGraph(List<Map<String, Object>> cmds) {
		Set<Integer> node_list = new HashSet<>();
		for (Map<String, Object> event : cmds) {
			boolean isCoordinator = event.get("node") == null;
			if (isCoordinator) {
				continue;
			}
			node_list.addAll((List<Integer>) event.get("node"));
		}
		for (int node_id : node_list) {
			g.addVertex(Integer.toString(node_id));
		}

		for (Map<String, Object> event : cmds) {
			boolean isCoordinator = event.get("node") == null;
			if (event.get("task").equals("loopTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> loopCmds = (ArrayList<Map<String, Object>>) args.get(1);
				DoGenerateNetworkGraph(loopCmds);
			} else if (event.get("task").equals("batchTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> batchCmds = (ArrayList<Map<String, Object>>) args.get(0);
				DoGenerateNetworkGraph(batchCmds);
			}

			List<Integer> node_id_list = (List<Integer>) event.get("node");
			if (event.get("task").equals("addNextHop")) {
				List<Integer> to_nodes = (List<Integer>) ((List<Object>) event.get("arguments")).get(1);
				List<Integer> stream_list = (List<Integer>) ((List<Object>) event.get("arguments")).get(0);
				for (int node_id : node_id_list) {
					for (int to_node : to_nodes) {
						StringBuilder streams = new StringBuilder();
						for (int stream_id : stream_list) {
							streams.append(stream_id).append(",");
						}
						ExposeEdge de = g.addEdge(Integer.toString(node_id), Integer.toString(to_node));
						de.setId(streams.toString().replaceAll(",$", ""));
					}
				}
			}

			if (isCoordinator) {
				continue;
			}
		}
	}*/

	//void GenerateNetworkGraph(List<Map<String, Object>> cmds) {
		//DoGenerateNetworkGraph(cmds);
	//}

	void ShowGraph() {
		// positioning via jgraphx layouts
		mxOrganicLayout layout = new mxOrganicLayout(applet.jgxAdapter);
		layout.setOptimizeEdgeLength(false);
		layout.execute(applet.jgxAdapter.getDefaultParent());

		JFrame frame = new JFrame();
		frame.getContentPane().add(applet, BorderLayout.CENTER);
		frame.setTitle("JGraphT Adapter to JGraphX Demo");
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		frame.pack();
		frame.setVisible(true);
		while (frame.isShowing()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

interface TaskTrigger {
	void eval();
}

interface MainTaskHandler {
	String HandleEvent(Map<String, Object> task);
}

public class Expose {
	private Map<String, Object> yaml_configuration;
	private boolean available = true;
	private NodeComm mc;
	List<Map<String, Object>> tasks;
	//GraphGenerator graphGenerator = new GraphGenerator();

	public static Expose expose;

	private int cmd_number = 1;

	Expose(String yaml_file) {
		this.yaml_configuration = Loadyaml(yaml_file);
	}

	class TaskRunner implements Runnable {
		Map<String, Object> task;

		TaskRunner(Map<String, Object> task) {
			this.task = task;
		}

		public void ExecuteTaskTrigger(Map<String, Object> task) {
			if (task.containsKey("trigger")) {
				((TaskTrigger)task.get("trigger")).eval();
				task.remove("trigger");
			}

			String task_name = (String) task.get("task");
			if (task_name.equals("batchTasks") || task_name.equals("loopTasks")) {
				// loopTasks have the inner tasks at index 1 where batchTasks have them at index 0
				int inner_tasks_index = task_name.equals("loopTasks") ? 1 : 0;
				List<Object> args = (List<Object>) task.get("arguments");
				List<Map<String, Object>> inner_tasks = (List<Map<String, Object>>) args.get(inner_tasks_index);
				for (Map<String, Object> inner_task : inner_tasks) {
					ExecuteTaskTrigger(inner_task);
				}
			}
		}

		public void doHandleEvent() {
			List<Integer> node_id_list = (List<Integer>) task.get("node");
			for (int node_id : node_id_list) {
				ExecuteTaskTrigger(task);
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
					case "showNetworkTopology": {
						//graphGenerator.ShowGraph();
						break;
					}
					default: {
						throw new RuntimeException("Unknown task " + task.get("task") + " parsed");
					}
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
		List<Integer> node_id_list = (List<Integer>) task.get("node");
		System.out.println("Line " + cmd_number + ": Node " + node_id_list + " executes " + task);
		for (int node_id : node_id_list) {
			task.put("node", Collections.singletonList(node_id));
			TaskRunner TaskRunner = new TaskRunner(task);
			if ((boolean) task.getOrDefault("parallel", false)) {
				new Thread(TaskRunner).start();
			} else {
				TaskRunner.doHandleEvent();
			}
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
		map.put("node", Collections.singletonList(node_id));
		mc.SendToSpe(map);

		//this.mc.nodeIdsToExperimentAPIs.get(node_id).AddTpIds(activeTracepointIds);

		List<Map<String, Object> >
				streamDefinitions = (ArrayList<Map<String, Object> >) yaml_configuration.get("stream-definitions");

		map = new HashMap<>();
		map.put("task", "addSchemas");
		map.put("arguments", streamDefinitions);
		map.put("node", Collections.singletonList(node_id));
		mc.SendToSpe(map);
		//this.mc.nodeIdsToExperimentAPIs.get(node_id).AddSchemas(streamDefinitions);
		System.out.println("Node " + node_id + ": AddSchemas " + streamDefinitions);
	}

	void WaitForSPEs(List<Map<String, Object>> cmds) {
		for (Map<String, Object> event : cmds) {
			boolean isCoordinator = event.get("node") == null;
			if (event.get("task").equals("loopTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> loopCmds = (ArrayList<Map<String, Object>>) args.get(1);
				WaitForSPEs(loopCmds);
			} else if (event.get("task").equals("batchTasks")) {
				List<Object> args = (List<Object>) event.get("arguments");
				List<Map<String, Object>> batchCmds = (ArrayList<Map<String, Object>>) args.get(0);
				WaitForSPEs(batchCmds);
			}
			List<Integer> node_id_list = (List<Integer>) event.get("node");
			if (isCoordinator) {
				continue;
			}
			for (int node_id : node_id_list) {
				//graphGenerator.AddVertex(node_id);
				while (!this.mc.nodeIdReady.getOrDefault(node_id, false)) {
					System.out.println("Waiting for Node " + node_id + " to connect");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(5);
					}
				}
			}
		}
	}

	public void SetTaskCallback(MainTaskHandler th) {
		this.mc.mainTaskHandler = th;
	}

	List<Map<String, Object>> PreprocessTask(Map<String, Object> raw_task) {
		List<Map<String, Object>> ret = new ArrayList<>();
		String cmd = (String) raw_task.get("task");
		List<Integer> node_id_list = (List<Integer>) raw_task.getOrDefault("node", Collections.singletonList(0));
		for (int node_id : node_id_list) {
			List<Object> args = (List<Object>) raw_task.get("arguments");

			List<Object> task_args = new ArrayList<>();

			Map<String, Object> map = new HashMap<>();
			for (String k : raw_task.keySet()) {
				map.put(k, raw_task.get(k));
			}
			map.put("node", Collections.singletonList(node_id));

			switch (cmd) {
				case "setTupleBatchSize": {
					task_args.add(args.get(0));
					break;
				}
				case "setIntervalBetweenTuples": {
					task_args.add(args.get(0));
					break;
				}
				case "wait": {
					task_args.add(args.get(0));
					break;
				}
				case "startRuntimeEnv": {
					break;
				}
				case "stopRuntimeEnv": {
					break;
				}
				case "setIntervalBetweenEvents": {
					task_args.add(args.get(0));
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
						List<Map<String, Object>> inner_task = PreprocessTask(inner_cmd);
						tasks.addAll(inner_task);
					}

					task_args.add(numberIterations);
					task_args.add(tasks);
					break;
				}
				case "batchTasks": {
					List<Map<String, Object>> cmds = (List<Map<String, Object>>) args.get(0);
					List<Map<String, Object>> tasks = new ArrayList<>();
					for (Map<String, Object> inner_cmd : cmds) {
						List<Map<String, Object>> inner_task = PreprocessTask(inner_cmd);
						tasks.addAll(inner_task);
					}

					task_args.add(tasks);
					break;
				}
				case "addNextHop": {
					List<Integer> streamId_list = (List<Integer>) args.get(0);
					List<Integer> nodeId_list = (List<Integer>) args.get(1);
					task_args.add(streamId_list);
					task_args.add(nodeId_list);
					//map.put("trigger", (TaskTrigger) () -> graphGenerator.AddEdge(node_id_list, streamId_list, nodeId_list));
					break;
				}
				case "writeStreamToCsv": {
					int stream_id = (int) args.get(0);
					String csvFolder = (String) args.get(1);
					task_args.add(stream_id);
					task_args.add(csvFolder);
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
							break;
						}
					}

					int iterations = (int) args.get(1);
					boolean realism = (boolean) args.get(2);
					task_args.add(iterations);
					task_args.add(realism);
					break;
				}
				case "sendDsAsConstantStream": {
					int dataset_id = (int) args.get(0);
					List<Map<String, Object>>
							datasets = (ArrayList<Map<String, Object>>) yaml_configuration.get("datasets");
					for (Map<String, Object> ds : datasets) {
						ds.put("file", ds.get("file"));
						if ((int) ds.get("id") == dataset_id) {
							task_args.add(ds);
							break;
						}
					}

					int desired_tuple_rate = (int) args.get(1);
					task_args.add(desired_tuple_rate);
					break;
				}
				case "sendDsAsVariableOnOffStream": {
					int dataset_id = (int) args.get(0);
					List<Map<String, Object>>
							datasets = (ArrayList<Map<String, Object>>) yaml_configuration.get("datasets");
					for (Map<String, Object> ds : datasets) {
						ds.put("file", ds.get("file"));
						if ((int) ds.get("id") == dataset_id) {
							task_args.add(ds);
							break;
						}
					}

					int desired_tuples_per_second = (int) args.get(1);
					int downtime = (int) args.get(2);
					int min = (int) args.get(3);
					int max = (int) args.get(4);
					int step = (int) args.get(5);
					task_args.add(desired_tuples_per_second);
					task_args.add(downtime);
					task_args.add(min);
					task_args.add(max);
					task_args.add(step);
					break;
				}
				case "clearQueries": {
					break;
				}
				case "setNidToAddress": {
					Map<Integer, Map<String, Object>> nodeIdToIpAndPort = (Map<Integer, Map<String, Object>>) args.get(0);
					task_args.add(nodeIdToIpAndPort);
					break;
				}
				case "endExperiment": {
					break;
				}
				case "addTpIds": {
					map.put("arguments", args);
					break;
				}
				case "retEndOfStream": {
					int nanoseconds = (int) args.get(0);
					task_args.add(nanoseconds);
					break;
				}
				case "retReceivedXTuples": {
					int number_tuples = (int) args.get(0);
					task_args.add(number_tuples);
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
				case "loadSavepoint": {
					String absolute_path = (String) args.get(0);
					task_args.add(absolute_path);
					break;
				}
				case "configure": {
					break;
				}
				case "moveQueryState": {
					int new_host = (int) args.get(0);
					task_args.add(new_host);
					break;
				}
				case "moveStaticQueryState": {
					int query_id = (int) args.get(0);
					int new_host = (int) args.get(1);
					task_args.add(query_id);
					task_args.add(new_host);
					break;
				}
				case "moveDynamicQueryState": {
					int query_id = (int) args.get(0);
					int new_host = (int) args.get(1);
					task_args.add(query_id);
					task_args.add(new_host);
					break;
				}
				case "resumeStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					task_args.add(stream_id_list);
					break;
				}
				case "stopStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					int migration_coordinator_node_id = (int) args.get(1);
					task_args.add(stream_id_list);
					task_args.add(migration_coordinator_node_id);
					break;
				}
				case "waitForStoppedStreams": {
					List<Integer> stopping_node_id_list = (List<Integer>) args.get(0);
					List<Integer> stream_id_list = (List<Integer>) args.get(1);
					task_args.add(stopping_node_id_list);
					task_args.add(stream_id_list);
					break;
				}
				case "bufferStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					task_args.add(stream_id_list);
					break;
				}
				/*case "bufferAndStopStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					task_args.add(stream_id_list);
					break;
				}
				case "bufferStopAndRelayStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					List<Integer> old_host_list = (List<Integer>) args.get(1);
					List<Integer> new_host_list = (List<Integer>) args.get(2);
					task_args.add(stream_id_list);
					task_args.add(old_host_list);
					task_args.add(new_host_list);
					// TODO: Replace edges in graph
					map.put("trigger", (TaskTrigger) () -> {
						graphGenerator.RemoveEdge(node_id_list, old_host_list);
						graphGenerator.AddEdge(node_id_list, stream_id_list, new_host_list);
					});
					break;
				}*/
				case "relayStream": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					List<Integer> old_host_list = (List<Integer>) args.get(1);
					List<Integer> new_host_list = (List<Integer>) args.get(2);
					task_args.add(stream_id_list);
					task_args.add(old_host_list);
					task_args.add(new_host_list);
					// TODO: Replace edges in graph
					map.put("trigger", (TaskTrigger) () -> {
						//graphGenerator.RemoveEdge(node_id_list, old_host_list);
						//graphGenerator.AddEdge(node_id_list, stream_id_list, new_host_list);
					});
					break;
				}
				case "removeNextHop": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					List<Integer> host_list = (List<Integer>) args.get(1);
					task_args.add(stream_id_list);
					task_args.add(host_list);
					break;
				}
				case "addSourceNodes": {
					int query_id = (int) args.get(0);
					List<Integer> stream_id_list = (List<Integer>) args.get(1);
					List<Integer> nid_list = (List<Integer>) args.get(2);
					task_args.add(query_id);
					task_args.add(stream_id_list);
					task_args.add(nid_list);
					break;
				}
				case "setAsPotentialHost": {
					List<Integer> stream_id_list = (List<Integer>) args.get(0);
					task_args.add(stream_id_list);
					break;
				}
				case "setTuplesPerSecondLimit": {
					int tuples_per_second = (int) args.get(0);
					List<Integer> node_list = (List<Integer>) args.get(1);
					task_args.add(tuples_per_second);
					task_args.add(node_list);
					break;
				}
				case "showNetworkTopology": {
					break;
				}
				default: {
					throw new RuntimeException("Unknown task " + cmd + " parsed");
				}
			}

			map.put("arguments", task_args);
			ret.add(map);
		}
		return ret;
	}

	void PreprocessTasks(List<Map<String, Object> > raw_tasks) {
		tasks = new ArrayList<>();
		for (Map<String, Object> raw_task : raw_tasks) {
			List<Map<String, Object>> task = PreprocessTask(raw_task);
			tasks.addAll(task);
		}
	}

	public List<Map<String, Object>> CollectMetrics(long metrics_window) {
		List<Map<String, Object>> metrics = new ArrayList<>();
		Map<String, Object> cmd = new HashMap<>();
		cmd.put("task", "collectMetrics");
		cmd.put("arguments", Collections.singletonList(metrics_window));
		List<String> yaml_metrics = this.mc.BroadcastTask(cmd);
		Yaml yaml = new Yaml();
		for (String yaml_metric : yaml_metrics) {
			synchronized (yaml) {
				metrics.add(yaml.load(StringEscapeUtils.unescapeJava(yaml_metric)));
			}
		}
		return metrics;
	}

	@SuppressWarnings("unchecked")
	void InterpretEvents(int eid) {
		System.out.println("Running from yaml config");
		List<Map<String, Object> >
			experiments = (ArrayList<Map<String, Object> >) yaml_configuration.get("experiments");
		for (Map<String, Object> experiment: experiments) {
			if (experiment.get("id").equals(eid)) {
				List<Map<String, Object> > cmds = (ArrayList<Map<String, Object> >) experiment.get("flow");
				PreprocessTasks(cmds);

				//GraphGenerator gg = new GraphGenerator();
				//gg.GenerateNetworkGraph(cmds);
				//gg.ShowGraph();

				// Ensure that all the necessary nodes are registered
				WaitForSPEs(cmds);
				// SetNidToAddress
				Map<String, Object> cmd = new HashMap<>();
				cmd.put("task", "setNidToAddress");
				List<Map<Integer, Map<String, Object>>> args = new ArrayList<>();
				args.add(mc.nodeIdsToClientInformation);
				cmd.put("arguments", args);
				mc.BroadcastTask(cmd);

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(20);
				}

				// All agents have registered, and now they can set up connections
				cmd = new HashMap<>();
				cmd.put("task", "configure");
				this.mc.BroadcastTask(cmd);

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

		this.EndExperimentTask();
	}

	public void EndExperimentTask() {
		Map<String, Object> cmd = new HashMap<>();
		cmd.put("task", "endExperiment");
		cmd.put("ack", false);
		this.mc.BroadcastTask(cmd);
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
		Map<String, Object> map;
		synchronized (yaml) {
			map = yaml.load(fis);
		}

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
		Expose.expose = jspef;

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
						synchronized (yaml) {
							map = yaml.load(reader.readLine());
						}
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

		jspef.mc = new NodeComm(cmd.getOptionValue("coordinator-ip"), Integer.parseInt(cmd.getOptionValue("coordinator-port")), jspef, 0);
		// coordinator port here is -1 of above (TEMP) and Expose is null (TEMP)
		// These are ongoing attempts to decouple and improve the communication
		// TODO: Make the port for the admin mc a cmd line argument
		//jspef.mc = new NodeComm(Integer.parseInt(cmd.getOptionValue("coordinator-port"))-1,  null, 0);
		new Thread(jspef.mc).start();
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
