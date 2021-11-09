package TestClient;

import Client.Client;
import Server.Interface.IResourceManager;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMITestClient extends Client implements Runnable
{
	private static String s_serverHost = "localhost";
	private static int s_serverPort = 1099;
	private static String s_serverName = "Middleware";

	public static int num_clients = 1;
	public static double throughput = 1.0; //load of x transactions / second
	public long start_time = 0; //in millisecond at default
	public long[][] results = new long[100][3];
	private static int runMode = 1; //1 or 2, 1 = oneRM, 2 = threeRMs
	//We analyze the last 100 transaction out of 200 transactions,
	// and we record the times of client side response time from start to end, middleware, and response time.

	//TODO: ADD YOUR GROUP NUMBER TO COMPILE (DONE)
	private static String s_rmiPrefix = "group_37_";

	private int getCurrThreadID(){
		return (int)(Thread.currentThread().getId());
	}


	@Override
	public void run() {
		int wait_time = (int) ((num_clients * 1000) / throughput);
		//If the submission time is at every 500, then it's actually submitted in an equally distributed interval, [500-x, 500+x]
		long abs_intervalBound = 50; //x

		//We run 200 transactions for each client
		for(int i = getCurrThreadID()*200; i < getCurrThreadID()*200 + 200; i++){
			double index = Math.random();
			int varied_SubmitTime = 0;
			if(index >= 0.5){
				//Math.random() returns equal to 0 or less than 1, so abs_intervalBound could be almost equal to 50 but never exceeds it
				varied_SubmitTime = wait_time - (int) (abs_intervalBound * Math.random());
			}else{
				varied_SubmitTime = wait_time + (int) (abs_intervalBound * Math.random());
			}

			try{
				long[] responseTime_list;
				long response_time = 0; //TODO: initialize the parameter with the actual response time.

				if(this.runMode == 1){

				}

				if(i >= getCurrThreadID()*200 + 100){ //Only analyze the results of the last 100 transactions

				}

				if(varied_SubmitTime - response_time < 0){
					continue;
				}
			}catch(Exception e){
				System.out.println("Exception while running the test analysis: " + e.getLocalizedMessage());
			}
		}

	}
	//TODO: finish these helper methods
	private long[] oneRM(){
		long[] response_times = new long[3];

		return response_times;
	}

	private long[] threeRMs(){
		long[] response_times = new long[3];

		return response_times;
	}

	private void setup_environment(){

	}


	public static void main(String args[])
	{
		if (args.length > 0)
		{
			num_clients = Integer.parseInt(args[0]);
			throughput = Integer.parseInt(args[1]);

			s_serverHost = args[0];
		}
		if (args.length > 1)
		{
			s_serverName = args[1];
		}
		if (args.length > 2)
		{
			System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0mUsage: java client.RMIClient [server_hostname [server_rmiobject]]");
			System.exit(1);
		}

		// Set the security policy
		if (System.getSecurityManager() == null)
		{
			System.setSecurityManager(new SecurityManager());
		}

		// Get a reference to the RMIRegister
		try {
			RMITestClient client = new RMITestClient();
			RMITestClient[] clients = new RMITestClient[num_clients];
			Thread[] threads = new Thread[num_clients];

			client.connectServer();
			client.start();
		} 
		catch (Exception e) {    
			System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public RMITestClient()
	{
		super();
	}

	public void connectServer()
	{
		connectServer(s_serverHost, s_serverPort, s_serverName);
	}

	public void connectServer(String server, int port, String name)
	{
		try {
			boolean first = true;
			while (true) {
				try {
					Registry registry = LocateRegistry.getRegistry(server, port);
					m_resourceManager = (IResourceManager)registry.lookup(s_rmiPrefix + name);
					System.out.println("Connected to '" + name + "' server [" + server + ":" + port + "/" + s_rmiPrefix + name + "]");
					break;
				}
				catch (NotBoundException|RemoteException e) {
					if (first) {
						System.out.println("Waiting for '" + name + "' server [" + server + ":" + port + "/" + s_rmiPrefix + name + "]");
						first = false;
					}
				}
				Thread.sleep(500);
			}
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}
	}

}

