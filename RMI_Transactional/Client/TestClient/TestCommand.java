package TestClient;

public enum TestCommand {
	Help("List all available commands", "[CommandName]"),

	RunAll("Runs all tests for Flight, Car and Room servers", ""),
	TestFlight("Tests basic functionality for the Flight server commands", ""),
	TestCar("Tests basic functionality for the Car server commands", ""),
	TestRoom("Tests basic functionality for the Room server commands", ""),

	Quit("Exit the test client application", "");

	String m_description;
	String m_args;

	TestCommand(String p_description, String p_args)
	{
		m_description = p_description;
		m_args = p_args;
	}

	public static TestCommand fromString(String string)
	{
		for (TestCommand cmd : TestCommand.values())
		{
			if (cmd.name().equalsIgnoreCase(string))
			{
				return cmd;
			}
		}
		throw new IllegalArgumentException("Command " + string + " not found");
	}

	public static String description()
	{
		String ret = "Commands supported by the client:\n";
		for (TestCommand cmd : TestCommand.values())
		{	 
			ret += "\t" + cmd.name() + "\n";
		}
		ret += "use help,<CommandName> for more detailed information";
		return ret;
	}

	public String toString()
	{
		String ret = name() + ": " + m_description + "\n";
		ret += "Usage: " + name() + "," + m_args;
		return ret;
	}
}             
