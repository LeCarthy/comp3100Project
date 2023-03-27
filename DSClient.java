import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class DSClient {

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

	static final String SERVER_IP = "localhost";
	static final int SERVER_PORT = 50000;

	static final boolean DEBUG = true;

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
		m_out.write(send_string.getBytes());
		return m_in.readLine();
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

	public int initialise_connection() throws IOException {
		String response = send_and_wait("HELO");
		if (!response.equals("OK")) {
			error_mismatch("OK", response);
			return 2;
		}

		String username = System.getProperty("user.name");
		response = send_and_wait(String.format("AUTH %s", username));
		if (!response.equals("OK")) {
			error_mismatch("OK", response);
			return 1;
		}

		return 0;
	}

	public void disconnect() throws IOException {
		send_and_wait("QUIT");
	}

	public void shutdown() throws IOException {
		m_out.close();
		m_socket.close();
	}

	public void collect_server_entries(ServerJob job) throws IOException {
		m_server_list.clear();
		String get_query = String.format("GETS Capable %d %d %d", job.m_core, job.m_memory, job.m_disk);
		String response = send_and_wait(get_query);
		if (!response.startsWith("DATA"))
			return;

		String[] data = response.split("\\s+");
		int record_count = Integer.parseInt(data[1]);

		m_out.write(("OK\n").getBytes());
		for (int i = 0; i < record_count; ++i) {
			m_server_list.add(new ServerListEntry(m_in.readLine()));
		}
		// why
		send_and_wait("OK");
	}

	private ArrayList<ServerListEntry> get_lrr_servers() {
		ArrayList<ServerListEntry> servers = new ArrayList<ServerListEntry>();

		// find the largest type
		ServerListEntry largest_type = m_server_list.get(0);
		for (int i = 0; i < m_server_list.size(); ++i) {
			if (largest_type.m_core < m_server_list.get(i).m_core)
				largest_type = m_server_list.get(i);
		}

		for (ServerListEntry server : m_server_list) {
			if (largest_type.m_type.equals(server.m_type)) {
				servers.add(server);
			}
		}

		return servers;
	}

	public ServerListEntry get_lrr() {

		if (m_server_list.size() == 0)
			return null;

		// if we don't have a list of largest servers, create one
		if (m_lrr_servers == null) {
			m_lrr_servers = get_lrr_servers();
		}

		// TODO update server statuses

		ServerListEntry server = m_lrr_servers.get(m_lrr_idx);
		if( ++m_lrr_idx >= m_lrr_servers.size())
		{
			m_lrr_idx = 0;
		}

		return server;
	}

	public void schedule_lrr(String job_str) throws IOException {
		ServerJob job = new ServerJob(job_str);

		// find a capable server
		collect_server_entries(job);
		ServerListEntry serv = get_lrr();
		if (serv != null) {
			send_and_wait(String.format("SCHD %d %s %d", job.m_id, serv.m_type, serv.m_id));
		}
	}

	public static void main(String[] args) throws IOException {

		DSClient client = new DSClient();
		client.connect();
		if (client.initialise_connection() != 0) {
			client.shutdown();
			return;
		}
		System.out.println("Started and initialised connection");

		String response = client.send_and_wait("REDY");

		while (!response.equals("NONE")) {
			if (response.startsWith("JCPL")) {
			} else if (response.startsWith("JOBN")) {

				// client.collect_server_records();
				client.schedule_lrr(response);

			}
			response = client.send_and_wait("REDY");
		}

		client.disconnect();
		client.shutdown();
	}
}