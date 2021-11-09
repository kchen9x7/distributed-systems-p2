package TestClient;

import Client.*;
import Server.Interface.IResourceManager;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Vector;

public class RMITestClient extends Client implements Runnable
{
	private static String s_serverHost = "localhost";
	private static int s_serverPort = 1099;
	private static String s_serverName = "Middleware";

	public static int num_clients = 1;
	public static double throughput = 1.0; //load of x transactions / second, 0 if
	public static long start_time = 0; //in millisecond at default
	public long[][] results = new long[100][6]; //Most commands return 6 part of data of time for each command for analyzing
	private static int runMode = 1; //1 or 2, 1 = oneRM, 2 = threeRMs
	//We analyze the last 100 transaction out of 200 transactions,
	// and we record the times of client side response time from start to end, middleware, and response time.

	//TODO: ADD YOUR GROUP NUMBER TO COMPILE (DONE)
	private static String s_rmiPrefix = "group_37_";

	private int getCurrThreadID(){
		return (int)(Thread.currentThread().getId());
	}

	//For each object who implemented Runnable, starting a thread will call its run()
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
				long total_response_time = 0; //for each transaction
				if(this.runMode == 1){
					responseTime_list = oneRM(i + getCurrThreadID()*200);
				}else{ //runMode == 2, many resourceManagers
					responseTime_list = threeRMs(i + getCurrThreadID()*200);
				}
				total_response_time = responseTime_list[0];

				if(i >= getCurrThreadID()*200 + 100){ //Only analyze the results of the last 100 transactions
					results[i - (getCurrThreadID()*200 + 100)] = responseTime_list;
				}
				if(varied_SubmitTime - total_response_time < 0 || throughput == 0){ //for only one type of RM, no sleep needed since no concurrent transaction
					continue;
				}
				Thread.sleep((int) (varied_SubmitTime - total_response_time));
			}catch(Exception e){
				System.out.println("Exception while running the test analysis: " + e.getLocalizedMessage());
			}
		}

	}
	//TODO: finish these helper methods
	//Returns:{TotalResponseTime, ClientTime, DBTime, MDWTime, RMTime, CommunicationTime}
	private long[] oneRM(int i) throws Exception{
		long startTime = System.currentTimeMillis();
		RMITestClient c = this;

		long[] startMid = c.execute(Command.Start, null);
		int xid = (int) startMid[0]; //transaction id

		Vector<String> add1 = new Vector<String>(4);
		add1.add(Integer.toString(xid));
		add1.add("Montreal" + i);
		add1.add(Integer.toString(50+i));
		add1.add(Integer.toString(100+i));

		Vector<String> qc1 = new Vector<String>(2);
		qc1.add(Integer.toString(xid));
		qc1.add("Montreal" + i);

		Vector<String> qcp1 = new Vector<String>(2);
		qcp1.add(Integer.toString(xid));
		qcp1.add("Montreal" + i);

		Vector<String> add2 = new Vector<String>(4);
		add2.add(Integer.toString(xid));
		add2.add("Toronto" + i);
		add2.add(Integer.toString(50 + i));
		add2.add(Integer.toString(200 + i));

		Vector<String> qc2 = new Vector<String>(2);
		qc2.add(Integer.toString(xid));
		qc2.add("Toronto" + i);

		Vector<String> qcp2 = new Vector<String>(2);
		qcp2.add(Integer.toString(xid));
		qcp2.add("Toronto" + i);

		Vector<String> add3 = new Vector<String>(4);
		add3.add(Integer.toString(xid));
		add3.add("Vancouver" + i);
		add3.add(Integer.toString(50 + i));
		add3.add(Integer.toString(300 + i));

		Vector<String> qc3 = new Vector<String>(2);
		qc3.add(Integer.toString(xid));
		qc3.add("Vancouver" + i);

		Vector<String> qcp3 = new Vector<String>(2);
		qcp3.add(Integer.toString(xid));
		qcp3.add("Vancouver" + i);

		long[] comm1 = c.execute(Command.AddCars, add1);
		long[] comm2 = c.execute(Command.QueryCars, qc1);
		long[] comm3 = c.execute(Command.QueryCarsPrice, qcp1);

		long[] comm4 = c.execute(Command.AddCars, add2);
		long[] comm5 = c.execute(Command.QueryCars, qc2);
		long[] comm6 = c.execute(Command.QueryCarsPrice, qcp2);

		long[] comm7 = c.execute(Command.AddCars, add3);
		long[] comm8 = c.execute(Command.QueryCars, qc3);
		long[] comm9 = c.execute(Command.QueryCarsPrice, qcp3);

		long[] commit = c.execute(Command.Commit, null);

		long total_responseTime = System.currentTimeMillis() - startTime; //transaction time
		long clientTime = startMid[6] + comm1[6] + comm2[6] + comm3[6] + comm4[6] + comm5[6] + comm6[6] + comm7[6] + comm8[6] + comm9[6] + commit[6];
		long DBTime = startMid[1] + comm1[1] + comm2[1] + comm3[1] + comm4[1] + comm5[1] + comm6[1] + comm7[1] + comm8[1] + comm9[1] + commit[1];
		long MDWTime = startMid[4] + comm1[4] + comm2[4] + comm3[4] + comm4[4] + comm5[4] + comm6[4] + comm7[4] + comm8[4] + comm9[4] + commit[4];
		long RMTime = startMid[2] + comm1[2] + comm2[2] + comm3[2] + comm4[2] + comm5[2] + comm6[2] + comm7[2] + comm8[2] + comm9[2] + commit[2];
		long communicateTime = startMid[5] + comm1[5] + comm2[5] + comm3[5] + comm4[5] + comm5[5] + comm6[5] + comm7[5] + comm8[5] + comm9[5] + commit[5]
				- MDWTime - RMTime - DBTime;

		return new long[] {total_responseTime, clientTime, DBTime, MDWTime, RMTime, communicateTime};
	}

	private long[] threeRMs(int i) throws Exception{
		long startTime = System.currentTimeMillis();
		RMITestClient c = this;

		long[] startMid = c.execute(Command.Start, null);
		int xid = (int) startMid[0]; //transaction id
		//flights
		Vector<String> add1 = new Vector<String>(4);
		add1.add(Integer.toString(xid));
		add1.add(Integer.toString(i)); //flight number
		add1.add(Integer.toString(200+i)); //num_seats
		add1.add(Integer.toString(300+i)); //price_seat

		Vector<String> qc1 = new Vector<String>(2);
		qc1.add(Integer.toString(xid));
		qc1.add(Integer.toString(i)); //flight number

		Vector<String> qcp1 = new Vector<String>(2);
		qcp1.add(Integer.toString(xid));
		qcp1.add(Integer.toString(i));
		//cars
		Vector<String> add2 = new Vector<String>(4);
		add2.add(Integer.toString(xid));
		add2.add("Calgary" + i);
		add2.add(Integer.toString(50 + i));
		add2.add(Integer.toString(200 + i));

		Vector<String> qc2 = new Vector<String>(2);
		qc2.add(Integer.toString(xid));
		qc2.add("Calgary" + i);

		Vector<String> qcp2 = new Vector<String>(2);
		qcp2.add(Integer.toString(xid));
		qcp2.add("Calgary" + i);
		//rooms
		Vector<String> add3 = new Vector<String>(4);
		add3.add(Integer.toString(xid));
		add3.add("Ottawa" + i);
		add3.add(Integer.toString(100 + i));
		add3.add(Integer.toString(200 + i));

		Vector<String> qc3 = new Vector<String>(2);
		qc3.add(Integer.toString(xid));
		qc3.add("Ottawa" + i);

		Vector<String> qcp3 = new Vector<String>(2);
		qcp3.add(Integer.toString(xid));
		qcp3.add("Ottawa" + i);

		long[] comm1 = c.execute(Command.AddFlight, add1);
		long[] comm2 = c.execute(Command.QueryFlight, qc1);
		long[] comm3 = c.execute(Command.QueryFlightPrice, qcp1);

		long[] comm4 = c.execute(Command.AddCars, add2);
		long[] comm5 = c.execute(Command.QueryCars, qc2);
		long[] comm6 = c.execute(Command.QueryCarsPrice, qcp2);

		long[] comm7 = c.execute(Command.AddRooms, add3);
		long[] comm8 = c.execute(Command.QueryRooms, qc3);
		long[] comm9 = c.execute(Command.QueryRoomsPrice, qcp3);

		long[] commit = c.execute(Command.Commit, null);

		long total_responseTime = System.currentTimeMillis() - startTime; //transaction time
		long clientTime = startMid[6] + comm1[6] + comm2[6] + comm3[6] + comm4[6] + comm5[6] + comm6[6] + comm7[6] + comm8[6] + comm9[6] + commit[6];
		long DBTime = startMid[1] + comm1[1] + comm2[1] + comm3[1] + comm4[1] + comm5[1] + comm6[1] + comm7[1] + comm8[1] + comm9[1] + commit[1];
		long MDWTime = startMid[4] + comm1[4] + comm2[4] + comm3[4] + comm4[4] + comm5[4] + comm6[4] + comm7[4] + comm8[4] + comm9[4] + commit[4];
		long RMTime = startMid[2] + comm1[2] + comm2[2] + comm3[2] + comm4[2] + comm5[2] + comm6[2] + comm7[2] + comm8[2] + comm9[2] + commit[2];
		long communicateTime = startMid[5] + comm1[5] + comm2[5] + comm3[5] + comm4[5] + comm5[5] + comm6[5] + comm7[5] + comm8[5] + comm9[5] + commit[5]
				- MDWTime - RMTime - DBTime;

		return new long[] {total_responseTime, clientTime, DBTime, MDWTime, RMTime, communicateTime};
	}

	public static void main(String args[])
	{
		if (args.length > 0)
		{
			num_clients = Integer.parseInt(args[0]);
			throughput = Integer.parseInt(args[1]);
		}
		if (args.length > 2)
		{
			runMode = Integer.parseInt(args[2]);
		}
		if (args.length > 3)
		{

			s_serverHost = args[3];
		}
		if (args.length > 4)
		{
			s_serverName = args[4];
		}
		if (args.length > 5)
		{
			System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0mUsage: java client.RMIClient [server_hostname [server_rmiobject]]");
			System.exit(1);
		}

		// Set the security policy
		if (System.getSecurityManager() == null)
		{
			System.setSecurityManager(new SecurityManager());
		}
		long startTime = 5*1000 + System.currentTimeMillis();
		// Get a reference to the RMIRegister
		try {
			RMITestClient[] clients = new RMITestClient[num_clients];
			Thread[] client_threads = new Thread[num_clients];

			for (int i = 0; i < num_clients; i++) {
				clients[i] = new RMITestClient();
				clients[i].num_clients = num_clients;
				clients[i].throughput = throughput;
				clients[i].start_time = startTime;
				clients[i].connectServer();

				client_threads[i] = new Thread(clients[i]);
				//TODO: Tentative solution, subject to change if wrong( changed from client_threads[i].start() to client_threads[i].run() )
				client_threads[i].run(); //In each wrong, calls oneRM() or threeRMs(), which contains calls to execute()
			}

			for (int i = 0; i < num_clients; i++) {
				client_threads[i].join();
			}
			System.out.println("ANALYZING DATA: \n\n");
			//TODO: below not sure if completely correct, subject to return
			for (int i = 0; i < num_clients; i++) {
				for (int j = 0; j < clients[i].results.length; j++) {
					for (int k = 0; k < clients[i].results[j].length; k++) {
						System.out.print(clients[i].results[j][k]);
						if (k != clients[i].results[j].length - 1)
							System.out.print(",");
					}
					System.out.println();
				}
				System.out.println();
			}
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

