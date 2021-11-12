// -------------------------------
// adapted from Kevin T. Manley
// CSE 593
// -------------------------------

package Server.RMI;

import Server.Interface.*;
import Server.Middleware.MiddlewareResourceManager;

import java.rmi.NotBoundException;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RMIMiddleware extends MiddlewareResourceManager
{
	private static String s_serverName = "Middleware";
	private static String s_rmiPrefix = "group_37_";
	private static int server_port = 1099;

	// default hostnames if not specified in arguments
	private static String flightServerHostName = "localhost";
	private static String carServerHostName = "localhost";
	private static String roomServerHostName = "localhost";

	public static void main(String args[])
	{
		if(args.length > 0){
			flightServerHostName = args[0];
		}
		if (args.length > 1) {
			carServerHostName = args[1];
		}
		if (args.length > 2) {
			roomServerHostName = args[2];
		}
			
		// Create the RMI server entry
		try {
			// Create a new Server object
			RMIMiddleware server = new RMIMiddleware(s_serverName);

			server.connectServers();

			// Dynamically generate the stub (client proxy)
			IResourceManager resourceManager = (IResourceManager)UnicastRemoteObject.exportObject(server, 0);

			// Bind the remote object's stub in the registry
			Registry l_registry;
			try {
				l_registry = LocateRegistry.createRegistry(1099);
			} catch (RemoteException e) {
				l_registry = LocateRegistry.getRegistry(1099);
			}
			final Registry registry = l_registry;
			registry.rebind(s_rmiPrefix + s_serverName, resourceManager);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						registry.unbind(s_rmiPrefix + s_serverName);
						System.out.println("'" + s_serverName + "' resource manager unbound");
					}
					catch(Exception e) {
						System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
						e.printStackTrace();
					}
				}
			});                                       
			System.out.println("'" + s_serverName + "' resource manager server ready and bound to '" + s_rmiPrefix + s_serverName + "'");

			System.out.println("\nMIDDLEWARE READY. LISTENING TO SERVE REQUESTS...\n");
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}

		// Create and install a security manager
		if (System.getSecurityManager() == null)
		{
			System.setSecurityManager(new SecurityManager());
		}

	}

	public RMIMiddleware(String name)
	{
		super(name);
	}

	public void connectServers()
	{
		System.out.println("Connecting to distributed servers:");

		String FLIGHT_SERVER_NAME = "Flight";
		System.out.println(FLIGHT_SERVER_NAME + " " + flightServerHostName + ":" + server_port + "/" + s_rmiPrefix + FLIGHT_SERVER_NAME);
		setFlightResourceManager(fetchServerProxy(flightServerHostName, server_port, FLIGHT_SERVER_NAME));

		String CAR_SERVER_NAME = "Car";
		System.out.println(CAR_SERVER_NAME + " " + carServerHostName + ":" + server_port + "/" + s_rmiPrefix + CAR_SERVER_NAME);
		setCarResourceManager(fetchServerProxy(carServerHostName, server_port, CAR_SERVER_NAME));

		String ROOM_SERVER_NAME = "Room";
		System.out.println(ROOM_SERVER_NAME + " " + roomServerHostName + ":" + server_port + "/" + s_rmiPrefix + ROOM_SERVER_NAME);
		setRoomResourceManager(fetchServerProxy(roomServerHostName, server_port, ROOM_SERVER_NAME));
	}

	public IRemoteResourceManager fetchServerProxy(String server, int port, String name)
	{
		IRemoteResourceManager serverProxy = null;
		try {
			boolean first = true;
			while (true) {
				try {
					Registry registry = LocateRegistry.getRegistry(server, port);
					serverProxy = (IRemoteResourceManager)registry.lookup(s_rmiPrefix + name);
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
		return serverProxy;
	}
}
