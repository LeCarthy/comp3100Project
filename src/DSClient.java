import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class DSClient {

	// Class for data recieved from GETS
	public class ServerListEntry {
		public String m_type;
		public int m_id;
		public String m_state;
		public int m_start_time;
		public int m_core;
		public int m_memory;
		public int m_disk;
		public int m_jobs_waiting;
		public int m_jobs_running;

		public ServerListEntry(String server) {
			String[] split_server = server.split("\\s+");
			this.m_type = split_server[0];
			this.m_id = Integer.parseInt(split_server[1]);
			this.m_state = split_server[2];
			this.m_start_time = Integer.parseInt(split_server[3]);
			this.m_core = Integer.parseInt(split_server[4]);
			this.m_memory = Integer.parseInt(split_server[5]);
			this.m_disk = Integer.parseInt(split_server[6]);
			this.m_jobs_waiting = Integer.parseInt(split_server[6]);
			this.m_jobs_running = Integer.parseInt(split_server[6]);
		}
	}

	// Class for data recieved from JOBN
	public class ServerJob {
		public int m_submitTime;
		public int m_id;
		public int estRuntime;
		public int m_core;
		public int m_memory;
		public int m_disk;

		public ServerJob(String job) {
			String[] job_split = job.split("\\s+");
			this.m_submitTime = Integer.parseInt(job_split[1]);
			this.m_id = Integer.parseInt(job_split[2]);
			this.estRuntime = Integer.parseInt(job_split[3]);
			this.m_core = Integer.parseInt(job_split[4]);
			this.m_memory = Integer.parseInt(job_split[5]);
			this.m_disk = Integer.parseInt(job_split[6]);
		}
	}

	enum AlgorthimType {
		ALG_LRR
	}

	static final String SERVER_IP = "localhost";
	static final int SERVER_PORT = 50000;

	static final boolean DEBUG = false;
	static final boolean PERFECT_WORLD = true; // 100% trust that the servers will be available

	Socket m_socket = null;
	BufferedReader m_in = null;
	DataOutputStream m_out = null;
	ArrayList<ServerListEntry> m_server_list = new ArrayList<ServerListEntry>();
	int m_lrr_idx = 0;
	ArrayList<ServerListEntry> m_lrr_servers = null;

	public DSClient() {
	}

	public String send_and_wait(String send) throws IOException {
		String send_string = send + "\n";

		if (DEBUG)
			System.out.println(String.format("JAVA SENT %s", send));

		m_out.write(send_string.getBytes());
		String rcvd_string = m_in.readLine();

		if (DEBUG)
			System.out.println(String.format("JAVA RCVD %s", rcvd_string));
		return rcvd_string;
	}

	private void error_mismatch(String expected, String actual) {
		System.out.println(
				String.format("ERROR: Expected response: \"%s\".\n" +
						"Recieved response: \"%s\"", expected, actual));
	}

	public void connect() throws IOException {
		m_socket = new Socket(SERVER_IP, SERVER_PORT);
		m_in = new BufferedReader(new InputStreamReader(m_socket.getInputStream()));
		m_out = new DataOutputStream(m_socket.getOutputStream());
	}

	// Greet the server and authorise connection
	public int connection_handshake() throws IOException {
		String response = send_and_wait("HELO");
		if (!response.equals("OK")) {
			error_mismatch("OK", response);
			return 2;
		}

		// Authorise with the current system username
		String username = System.getProperty("user.name");
		response = send_and_wait(String.format("AUTH %s", username));
		if (!response.equals("OK")) {
			error_mismatch("OK", response);
			return 1;
		}

		System.out.println("Started and initialised connection");
		return 0;
	}

	public void disconnect() throws IOException {
		String quit_response = send_and_wait("QUIT");
		if (!quit_response.equals("QUIT")) {
			error_mismatch("QUIT", quit_response);
		}
	}

	public void shutdown() throws IOException {
		m_out.close();
		m_socket.close();
	}

	private void find_capable_servers(ServerJob job) throws IOException {
		// In a perfect world the servers are always healthy, don't query the server.
		if( PERFECT_WORLD && !m_server_list.isEmpty() )
			return;
		
		m_server_list.clear();
		String get_query = String.format("GETS Capable %d %d %d", job.m_core, job.m_memory, job.m_disk);
		String response = send_and_wait(get_query);
		if (!response.startsWith("DATA"))
			return;

		String[] data = response.split("\\s+");
		int record_count = Integer.parseInt(data[1]);

		// send_and_wait only returns readline so we need to write and read each line proper
		m_out.write(("OK\n").getBytes());
		for (int i = 0; i < record_count; ++i) {
			m_server_list.add(new ServerListEntry(m_in.readLine()));
		}

		// tell the server we've recieved the list
		send_and_wait("OK");
	}

	// Get the server based on the Largest-Round-Robin algorithm
	private ServerListEntry get_server_lrr() {
		// if we don't have a list of largest servers, create one
		if (m_lrr_servers == null) {
			m_lrr_servers = new ArrayList<ServerListEntry>();

			// find the largest type
			ServerListEntry largest_type = m_server_list.get(0);
			for (int i = 0; i < m_server_list.size(); ++i) {
				if (largest_type.m_core < m_server_list.get(i).m_core)
					largest_type = m_server_list.get(i);
			}

			for (ServerListEntry server : m_server_list) {
				if (largest_type.m_type.equals(server.m_type)) {
					m_lrr_servers.add(server);
				}
			}
		}

		if( !PERFECT_WORLD )
		{
			// TODO update LRR servers with latest changes
		}

		// return our current server
		ServerListEntry server = m_lrr_servers.get(m_lrr_idx++);
		if (m_lrr_idx >= m_lrr_servers.size()) {
			m_lrr_idx = 0;
		}

		return server;
	}

	private ServerListEntry get_server(AlgorthimType alg) {

		if (m_server_list.size() == 0)
			return null;

		ServerListEntry server = null;
		switch (alg) {
			case ALG_LRR:
				server = get_server_lrr();
				break;
			default:
				System.out.println("Unhandled Algorithm type");
				break;
		}

		return server;
	}

	public void schedule_job(String job_str, AlgorthimType alg) throws IOException {
		ServerJob job = new ServerJob(job_str);

		find_capable_servers(job);
		// get the latest server based on our chosen algorithm
		ServerListEntry serv = get_server(alg);

		if (serv != null) {
			// Send the schedule request
			send_and_wait(String.format("SCHD %d %s %d", job.m_id, serv.m_type, serv.m_id));
		}
	}

	public static void main(String[] args) throws IOException {

		DSClient client = new DSClient();
		client.connect();
		if (client.connection_handshake() != 0) {
			client.shutdown();
			return;
		}

		String response = client.send_and_wait("REDY");
		while (!response.equals("NONE")) {
			if (response.startsWith("JCPL")) { // Job completion msg
				// TODO something?
			} else if (response.startsWith("JOBN")) { // Job to be scheduled
				client.schedule_job(response, AlgorthimType.ALG_LRR);
			}
			response = client.send_and_wait("REDY");
		}

		client.disconnect();
		client.shutdown();
	}
}