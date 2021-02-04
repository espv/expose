package no.uio.ifi;

import org.apache.commons.text.StringEscapeUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;


public class NodeComm extends Comm implements Runnable {
	private ServerSocket server;
	private Expose expose;
	private boolean isRunning = true;
	Map<Integer, NodeClient> nodeIdsToClients = new HashMap<>();
	Map<Integer, Boolean> nodeIdReady = new HashMap<>();
	Map<Integer, Map<String, Object> > nodeIdsToClientInformation = new HashMap<>();
	int local_node_id;
	String ip;
	int client_port;
	int local_coordinator_port;
	int state_transfer_port;
	int cur_sequence_id = 0;
	MainTaskHandler mainTaskHandler;

	int MAIN_COORDINATOR_NODE_ID = 0;

	NodeComm(String ip, int local_coordinator_port, int client_port, int state_transfer_port, Expose expose, int local_node_id) {
		this.ip = ip;
		this.local_node_id = local_node_id;
		try {
			server = new ServerSocket(local_coordinator_port);
		} catch (IOException e) {
			e.printStackTrace();
			ShutDown();
		}
		this.expose = expose;
		this.local_coordinator_port = local_coordinator_port;
		this.client_port = client_port;
		this.state_transfer_port = state_transfer_port;
	}

	NodeComm(String ip, int local_coordinator_port, Expose expose, int local_node_id) {
		this.ip = ip;
		this.local_node_id = local_node_id;
		try {
			server = new ServerSocket(local_coordinator_port);
		} catch (IOException e) {
			e.printStackTrace();
			ShutDown();
		}
		this.expose = expose;
		this.local_coordinator_port = local_coordinator_port;
	}

	// Put this method in a different class
	public String SendToSpe(Map<String, Object> t) {
		Map<String, Object> task = new HashMap<>(t);
		int sequence_id;
		if (task.containsKey("sequence-id")) {
			sequence_id = (int) task.get("sequence-id");
		} else {
			sequence_id = cur_sequence_id++;
			task.put("sequence-id", sequence_id);
		}
		task.put("type", "task");

		List<Integer> node_id_list = (List<Integer>) task.get("node");
		StringBuilder ret = new StringBuilder();
		for (int node_id : node_id_list) {
			task.put("node", Collections.singletonList(node_id));
			NodeClient mc = nodeIdsToClients.get(node_id);
			synchronized (mc) {
				SendMap(task, mc.out);
			}
			boolean expectAck = (boolean) task.getOrDefault("ack", true);
			if (expectAck) {
				while (!mc.received_replies.containsKey(sequence_id)) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				Map<String, Object> reply = mc.received_replies.remove(sequence_id);
				String response = (String) reply.get("response");
				ret.append(response);
			}
		}
		return ret.toString();
	}

	public Map<Integer, NodeClient> GetNodeIdsToClients() {return nodeIdsToClients;}

	public void ShutDown() {
		try {
			this.server.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public List<String> BroadcastTask(Map<String, Object> cmd) {
		int sequence_id = cur_sequence_id++;
		cmd.put("sequence-id", sequence_id);
		List<String> res = new ArrayList<>();
		List<NodeClient> nodeClients = new ArrayList<>(nodeIdsToClients.values());
		for (int i = 0; i < nodeClients.size(); i++) {
			cmd.put("node", Collections.singletonList(nodeClients.get(i).remote_node_id));
			res.add(SendToSpe(cmd));
		}
		return res;
	}

	public void SetupClientMap(NodeClient client, int nodeId, String ip, int coordinator_port, int client_port, int state_transfer_port) {
		Map<String, Object> address = new HashMap<>();
		address.put("ip", ip);
		address.put("client-port", client_port);
		address.put("spe-coordinator-port", coordinator_port);
		address.put("state-transfer-port", state_transfer_port);
		try {
			address.put("ip", java.net.InetAddress.getByName((String) address.get("ip")).getHostAddress());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(30);
		}
		nodeIdsToClientInformation.put(nodeId, address);
		nodeIdsToClients.put(nodeId, client);
	}

	public void ConnectToNode(int nodeId, String nodeIp, int nodeCoordinatorPort) {
		if (nodeIdsToClients.containsKey(nodeId)) {
			System.out.println("We are already connected to Node " + nodeId);
			return;
		}
		NodeClient client;
		try {
			client = new NodeClient(new Socket(nodeIp, nodeCoordinatorPort));
			client.remote_node_id = nodeId;
		} catch (IOException e) {
			e.printStackTrace();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
			return;
		}
		System.out.println("New client's node ID: " + nodeId + ", address: " + nodeIp + ":" + nodeCoordinatorPort);
		client.out.write(this.local_node_id           + "\n" +
				this.ip          + "\n" +
				this.client_port + "\n" +
				this.local_coordinator_port + "\n" +
				this.state_transfer_port + "\n");
		client.out.flush();

		SetupClientMap(client, nodeId, nodeIp, nodeCoordinatorPort, -1, -1);

		new Thread(client).start();
	}

	public void run() {
		System.out.println("NodeComm server is running at 127.0.0.1:" + local_coordinator_port);
		while (isRunning) {
			NodeClient client;
			try {
				client = new NodeClient(server.accept());
			} catch (IOException e) {
				if (!isRunning) {
					// We closed the server socket and exited gracefully
					break;
				}
				e.printStackTrace();
				try {
					Thread.sleep(2000);
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
				continue;
			}
			int nodeId = client.getNodeId();
			client.remote_node_id = nodeId;
			String nodeIp = client.getIp();
			int nodeClientPort = client.getPort();
			int nodeCoordinatorPort = client.getPort();
			int nodeStateTransferPort = client.getPort();
			SetupClientMap(client, nodeId, nodeIp, nodeCoordinatorPort, nodeClientPort, nodeStateTransferPort);
			new Thread(client).start();

			if (this.MAIN_COORDINATOR_NODE_ID == this.local_node_id) {
				this.expose.Configure(nodeId);
				// This node is now ready to participate in the experiment
				this.nodeIdReady.put(nodeId, true);
			}
		}
		System.out.println("NodeComm server is exiting");
	}

	class NodeClient implements Runnable {
		private Socket socket;
		public PrintWriter out;
		public OutputStream outputStream;
		public BufferedReader in;
		int remote_node_id;
		List<Map<String, Object>> received_tasks = new ArrayList<>();
		Map<Integer, Map<String, Object>> received_replies = new HashMap<>();
		MessageHandler messageHandler = new MessageHandler();


		public NodeClient(Socket socket) throws IOException {
			this.socket = socket;
			this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			this.outputStream = socket.getOutputStream();
			this.out = new PrintWriter(outputStream);
		}

		class MessageHandler implements Runnable {

			@Override
			public void run() {
				while (true) {
					Map<String, Object> msg;
					try {
						msg = receiveMap(in, yaml);
					} catch (Exception e) {
						e.printStackTrace();
						ShutDown();
						return;
					}

					if (msg.get("type").equals("response")) {
						received_replies.put((int) msg.get("sequence-id"), msg);
					} else if (msg.get("type").equals("task")) {
						received_tasks.add(msg);
					} else {
						System.err.println("Undefined message type " + msg.get("type") + " in received message: " + msg);
						System.exit(52);
					}
				}
			}
		}

		public void run() {
			new Thread(this.messageHandler).start();
			while (true) {
				Map<String, Object> cmd;
				while (received_tasks.isEmpty()) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				cmd = received_tasks.remove(0);
				int sequence_id = (int) cmd.get("sequence-id");
				new Thread(() -> {
					String response = mainTaskHandler.HandleEvent(cmd);
					boolean requireAck = (boolean) cmd.getOrDefault("ack", true);
					try {
						if (requireAck) {
							Map<String, Object> reply = new HashMap<>();
							reply.put("sequence-id", sequence_id);
							reply.put("response", response);
							reply.put("type", "response");
							synchronized (this.out) {
								synchronized (yaml) {
									this.out.write(StringEscapeUtils.escapeJava(yaml.dump(reply)) + "\n");
								}
								this.out.flush();
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						ShutDown();
					}
				}).start();
			}
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
