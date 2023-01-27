package no.uio.ifi;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
	private Map<Integer, Map<String, Object> > nodeIdsToClientInformation = new HashMap<>();
	Map<Integer, CoordinatorExperimentAPI > nodeIdsToExperimentAPIs = new HashMap<>();

	CoordinatorComm(int port, Expose expose) {
		try {
			server = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
			ShutDown();
		}
		this.expose = expose;
	}

	// Put this method in a different class
	public String SendToSpe(Task cmd, int node_id) {
		try {
			CoordinatorClient mc = nodeIdsToClients.get(node_id);
			SendMap(cmd.event, mc.out);
			boolean expectAck = (boolean) cmd.event.getOrDefault("ack", true);
			String ret = null;
			if (expectAck) {
				ret = mc.in.readLine();
			}
			return ret;
		} catch (IOException e) {
			ShutDown();
			return e.toString();
		}
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
		cmd.put("task", "EndExperiment");
		cmd.put("ack", false);
		SendCoordinatorTask(cmd);
	}

	public void run() {
		System.out.println("CoordinatorServer is running");
		while (isRunning) {
			CoordinatorClient client = null;
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
			assert client != null;
			int nodeId = client.getNodeId();
			String clientIp = client.getIp();
			int clientPort = client.getPort();
			Map<String, Object> address = new HashMap<>();
			address.put("ip", clientIp);
			address.put("port", clientPort);
			System.out.println("New client's node ID: " + nodeId + ", address: " + clientIp + ":" + clientPort);
			try {
				address.put("ip", java.net.InetAddress.getByName((String) address.get("ip")).getHostAddress());
			} catch (UnknownHostException e) {
				e.printStackTrace();
				System.exit(30);
			}
			nodeIdsToClientInformation.put(nodeId, address);
			nodeIdsToClients.put(nodeId, client);
			nodeIdsToExperimentAPIs.put(nodeId, new CoordinatorExperimentAPI(this, nodeId));
			Map<String, Object> cmd = new HashMap<>();
			cmd.put("task", "SetNidToAddress");
			List<Map<Integer, Map<String, Object>>> args = new ArrayList<>();
			args.add(this.nodeIdsToClientInformation);
			cmd.put("arguments", args);
			SendCoordinatorTask(cmd);
			this.expose.Configure(nodeId);

			// This node is now ready to participate in the experiment
			nodeIdReady.put(nodeId, true);
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
				nodeId = Integer.parseInt(in.readLine());
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
				port = Integer.parseInt(in.readLine());
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(7);
			}
			return port;
		}
	}
}
