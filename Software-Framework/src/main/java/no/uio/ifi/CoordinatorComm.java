package no.uio.ifi;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CoordinatorComm extends Comm implements Runnable {
	private ServerSocket server;
	private Expose expose;
	private boolean isRunning = true;
	Map<Integer, CoordinatorClient> nodeIdsToClients = new HashMap<>();
	Map<Integer, Boolean> nodeIdReady = new HashMap<>();
	Map<Integer, Map<String, Object> > nodeIdsToClientInformation = new HashMap<>();
	//Map<Integer, ExperimentAPI> nodeIdsToExperimentAPIs = new HashMap<>();
	int local_node_id;
	int port;

	CoordinatorComm(int port, Expose expose, int local_node_id) {
		this.local_node_id = local_node_id;
		try {
			server = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
			ShutDown();
		}
		this.expose = expose;
		this.port = port;
	}

	// Put this method in a different class
	public String SendToSpe(Map<String, Object> task) {
		List<Integer> node_id_list = (List<Integer>) task.get("node");
		StringBuilder ret = new StringBuilder();
		for (int node_id : node_id_list) {
			try {
				CoordinatorClient mc = nodeIdsToClients.get(node_id);
				SendMap(task, mc.out);
				boolean expectAck = (boolean) task.getOrDefault("ack", true);
				if (expectAck) {
					ret.append(mc.in.readLine()).append(", ");
				}
			} catch (IOException e) {
				ShutDown();
				return e.toString();
			}
		}
		return ret.toString();
	}

	public Map<Integer, CoordinatorClient> GetNodeIdsToClients() {return nodeIdsToClients;}

	public void PrintIncomingOfClient(int nodeId) {
		CoordinatorClient client = nodeIdsToClients.get(nodeId);
		try {
			System.out.println(client.in.readLine());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void ShutDown() {
		try {
			this.server.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public void SendCoordinatorTask(Map<String, Object> cmd) {
		cmd.put("administrative", true);
		for (CoordinatorClient mc : nodeIdsToClients.values()) {
			try {
				SendMap(cmd, mc.out);
				boolean expectAck = (boolean) cmd.getOrDefault("ack", true);
				if (expectAck) {
					mc.in.readLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void EndExperimentTask() {
		Map<String, Object> cmd = new HashMap<>();
		cmd.put("task", "endExperiment");
		cmd.put("ack", false);
		SendCoordinatorTask(cmd);
	}

	public void run() {
		System.out.println("CoordinatorServer is running at 127.0.0.1:" + port);
		while (isRunning) {
			CoordinatorClient client;
			try {
				client = new CoordinatorClient(server.accept());
			} catch (IOException e) {
				if (!isRunning) {
					// We closed the server socket and exited gracefully
					break;
				}
				e.printStackTrace();
				continue;
			}
			int nodeId = client.getNodeId();
			String speIp = client.getIp();
			int speClientPort = client.getPort();
			int speCoordinatorPort = client.getPort();
			Map<String, Object> address = new HashMap<>();
			address.put("ip", speIp);
			address.put("client-port", speClientPort);
			address.put("spe-coordinator-port", speCoordinatorPort);
			System.out.println("New client's node ID: " + nodeId + ", address: " + speIp + ":" + speClientPort);
			nodeIdsToClientInformation.put(nodeId, address);
			nodeIdsToClients.put(nodeId, client);
			//nodeIdsToExperimentAPIs.put(nodeId, new CoordinatorExperimentAPI(this, nodeId));

			if (this.expose != null) {
				this.expose.Configure(nodeId);
				// This node is now ready to participate in the experiment
				nodeIdReady.put(nodeId, true);
			}
		}
		System.out.println("CoordinatorServer is exiting");
	}

	class CoordinatorClient {
		private Socket socket;
		public PrintWriter out;
		public BufferedReader in;

		public CoordinatorClient(Socket socket) throws IOException {
			this.socket = socket;
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out = new PrintWriter(socket.getOutputStream());
		}

		public Socket getSocket() {
			return socket;
		}

		public int getNodeId() {
			int nodeId = -1;
			try {
				String l = in.readLine();
				nodeId = Integer.parseInt(l);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(3);
			}
			return nodeId;
		}

		public String getIp() {
			String ip = "";
			try {
				ip = in.readLine();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(8);
			}
			return ip;
		}

		public int getPort() {
			int port = -1;
			try {
				String l = in.readLine();
				port = Integer.parseInt(l);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(7);
			}
			return port;
		}
	}
}
