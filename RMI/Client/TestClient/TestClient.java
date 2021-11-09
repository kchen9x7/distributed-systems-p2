package TestClient;

import Server.Exception.InvalidTransactionException;
import Server.Exception.TransactionAbortedException;
import Server.Interface.IResourceManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.rmi.UnmarshalException;
import java.util.StringTokenizer;
import java.util.Vector;

public abstract class TestClient
{
	IResourceManager m_resourceManager = null;

	public TestClient()
	{
		super();
	}

	public abstract void connectServer();

	public void start()
	{
		// Prepare for reading commands
		System.out.println();
		System.out.println("Location \"help\" for list of supported commands");

		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

		while (true)
		{
			// Read the next command
			String command = "";
			Vector<String> arguments = new Vector<String>();
			try {
				System.out.print((char)27 + "[32;1m\n>] " + (char)27 + "[0m");
				command = stdin.readLine().trim();
			}
			catch (IOException io) {
				System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0m" + io.getLocalizedMessage());
				io.printStackTrace();
				System.exit(1);
			}

			try {
				arguments = parse(command);
				TestCommand cmd = TestCommand.fromString((String)arguments.elementAt(0));
				try {
					execute(cmd, arguments);
				}
				catch (ConnectException e) {
					connectServer();
					execute(cmd, arguments);
				}
			}
			catch (IllegalArgumentException|ServerException e) {
				System.err.println((char)27 + "[31;1mCommand exception: " + (char)27 + "[0m" + e.getLocalizedMessage());
			}
			catch (ConnectException|UnmarshalException e) {
				System.err.println((char)27 + "[31;1mCommand exception: " + (char)27 + "[0mConnection to server lost");
			}
			catch (Exception e) {
				System.err.println((char)27 + "[31;1mCommand exception: " + (char)27 + "[0mUncaught exception");
				e.printStackTrace();
			}
		}
	}

	public void execute(TestCommand cmd, Vector<String> arguments)
			throws RemoteException, NumberFormatException, InvalidTransactionException, TransactionAbortedException {
		switch (cmd)
		{
			case Help:
			{
				if (arguments.size() == 1) {
					System.out.println(TestCommand.description());
				} else if (arguments.size() == 2) {
					TestCommand l_cmd = TestCommand.fromString((String)arguments.elementAt(1));
					System.out.println(l_cmd.toString());
				} else {
					System.err.println((char)27 + "[31;1mCommand exception: " + (char)27 + "[0mImproper use of help command. Location \"help\" or \"help,<CommandName>\"");
				}
				break;
			}
			case TestFlight: {
				checkArgumentsCount(1, arguments.size());
				System.out.println("\nTesting Flight server functionality. . .");
				System.out.println("TESTING AddFlight");
				if(m_resourceManager.addFlight(1, 747, 50, 250)){
					System.out.println((char)27 + "[32;1mPASSED: Flight added" + (char)27 + "[0m");
					if(m_resourceManager.queryFlight(1, 747)==50){
						System.out.println((char)27 + "[32;1mPASSED: Correct flight seats returned from server" + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Incorrect number of flight seats returned.");
					}
					if(m_resourceManager.queryFlightPrice(1, 747)==250){
						System.out.println((char)27 + "[32;1mPASSED: Correct flight price returned from server" + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Incorrect number of flight price returned.");
					}
				} else {
					System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to add flight.");
				}
				System.out.println("TESTING DeleteFlight");
				if(m_resourceManager.deleteFlight(1, 747)){
					if(m_resourceManager.queryFlight(1, 747)==0){
						System.out.println((char)27 + "[32;1mPASSED: Flight deleted " + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to delete flight.");
					}
				} else{
					System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to delete flight.");
				}
				break;
			}
			case TestCar: {
				checkArgumentsCount(1, arguments.size());
				System.out.println("\nTesting Car server functionality. . .");
				System.out.println("TESTING AddCars");
				if(m_resourceManager.addCars(1, "Montreal", 35, 25)){
					System.out.println((char)27 + "[32;1mPASSED: Cars added" + (char)27 + "[0m");
					if(m_resourceManager.queryCars(1, "Montreal")==35){
						System.out.println((char)27 + "[32;1mPASSED: Correct number of cars returned from server" + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Incorrect number of cars returned.");
					}
					if(m_resourceManager.queryCarsPrice(1, "Montreal")==25){
						System.out.println((char)27 + "[32;1mPASSED: Correct car price returned from server" + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Incorrect car price returned.");
					}
				} else {
					System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to add cars.");
				}
				System.out.println("TESTING DeleteCars");
				if(m_resourceManager.deleteCars(1, "Montreal")){
					if(m_resourceManager.queryCars(1, "Montreal")==0){
						System.out.println((char)27 + "[32;1mPASSED: Cars deleted " + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to delete cars.");
					}
				} else{
					System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to delete cars.");
				}
				break;
			}
			case TestRoom: {
				checkArgumentsCount(1, arguments.size());
				System.out.println("\nTesting Room server functionality. . .");
				System.out.println("TESTING AddRooms");
				if(m_resourceManager.addRooms(1, "Montreal", 35, 25)){
					System.out.println((char)27 + "[32;1mPASSED: Rooms added" + (char)27 + "[0m");
					if(m_resourceManager.queryRooms(1, "Montreal")==35){
						System.out.println((char)27 + "[32;1mPASSED: Correct number of rooms returned from server" + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Incorrect number of rooms returned.");
					}
					if(m_resourceManager.queryRoomsPrice(1, "Montreal")==25){
						System.out.println((char)27 + "[32;1mPASSED: Correct room price returned from server" + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Incorrect room price returned.");
					}
				} else {
					System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to add rooms.");
				}
				System.out.println("TESTING DeleteRooms");
				if(m_resourceManager.deleteRooms(1, "Montreal")){
					if(m_resourceManager.queryRooms(1, "Montreal")==0){
						System.out.println((char)27 + "[32;1mPASSED: Rooms deleted " + (char)27 + "[0m");
					} else{
						System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to delete rooms.");
					}
				} else{
					System.err.println((char)27 + "[31;1mTest failed! " + (char)27 + "Server failed to delete rooms.");
				}
				break;
			}
			case RunAll:{
				checkArgumentsCount(1, arguments.size());

				Vector<String> args = new Vector<String>();
				args.add("TestArgument");
				execute(TestCommand.TestFlight, args);
				execute(TestCommand.TestCar, args);
				execute(TestCommand.TestRoom, args);
				break;
			}
			case Quit:
				checkArgumentsCount(1, arguments.size());

				System.out.println("Quitting client");
				System.exit(0);
		}
	}

	public static Vector<String> parse(String command)
	{
		Vector<String> arguments = new Vector<String>();
		StringTokenizer tokenizer = new StringTokenizer(command,",");
		String argument = "";
		while (tokenizer.hasMoreTokens())
		{
			argument = tokenizer.nextToken();
			argument = argument.trim();
			arguments.add(argument);
		}
		return arguments;
	}

	public static void checkArgumentsCount(Integer expected, Integer actual) throws IllegalArgumentException
	{
		if (expected != actual)
		{
			throw new IllegalArgumentException("Invalid number of arguments. Expected " + (expected - 1) + ", received " + (actual - 1) + ". Location \"help,<CommandName>\" to check usage of this command");
		}
	}

	public static int toInt(String string) throws NumberFormatException
	{
		return (Integer.valueOf(string)).intValue();
	}

	public static boolean toBoolean(String string)// throws Exception
	{
		return (Boolean.valueOf(string)).booleanValue();
	}
}
