package Server.Middleware;

import Server.Common.*;
import Server.Interface.*;

import java.util.*;
import java.rmi.RemoteException;

import Server.Transactions.*;
//TODO: Add locking stuff
public class MiddlewareResourceManager implements IResourceManager
{
	protected String m_name = "";
	protected RMHashMap m_data = new RMHashMap();

	protected TransactionManager transactionManager;
	protected MiddlewareTransactionHandler middleware_tm;
	protected MiddlewareResourceManager middleware_rm;
	private int time_allowed = 60; //default time allowed for a transaction in seconds

	private IResourceManager flightResourceManager = null;
	private IResourceManager carResourceManager = null;
	private IResourceManager roomResourceManager = null;

	private void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	private void updateAllowedTime(int transactionID){
		middleware_tm.readActiveTransaction(transactionID).updateLastOperationTime();
	}

	public MiddlewareResourceManager(String p_name)
	{
		m_name = p_name;
	}

	protected void setFlightResourceManager(IResourceManager flightResourceManager){
		this.flightResourceManager = flightResourceManager;
	}

	protected void setCarResourceManager(IResourceManager carResourceManager){
		this.carResourceManager = carResourceManager;
	}

	protected void setRoomResourceManager(IResourceManager roomResourceManager){
		this.roomResourceManager = roomResourceManager;
	}

	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException
	{
		Trace.info("Request for AddFlight received from Client.");
        Trace.info("Forwarding request to Flight Server...");
        if (flightResourceManager.addFlight(xid, flightNum, flightSeats, flightPrice)) {
            Trace.info("Flight server completed request successfully!");
            return true;
		} else {
            Trace.info("Flight server failed to complete request.");
            return false;
        }
	}

	public boolean addCars(int xid, String location, int count, int price) throws RemoteException
	{
		Trace.info("Request for AddCars received from Client.");
        Trace.info("Forwarding request to Car Server...");
        if (carResourceManager.addCars(xid, location, count, price)) {
            Trace.info("Car server completed request successfully!");
            return true;
		} else {
            Trace.info("Car server failed to complete request.");
            return false;
        }
	}

	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException
	{
		Trace.info("Request for AddRooms received from Client.");
        Trace.info("Forwarding request to Room Server...");
        if (roomResourceManager.addRooms(xid, location, count, price)) {
            Trace.info("Room server completed request successfully!");
            return true;
		} else {
            Trace.info("Room server failed to complete request.");
            return false;
        }
	}

	public int newCustomer(int xid) throws RemoteException
	{
		Trace.info("Request for AddRooms received from Client.");
		Trace.info("Replicating customer information on distributed servers...");

		int cid = Integer.parseInt(String.valueOf(xid) +
		String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
		String.valueOf(Math.round(Math.random() * 100 + 1)));

		boolean isFlightCustomerAdded = flightResourceManager.newCustomer(xid, cid);
		boolean isCarCustomerAdded = carResourceManager.newCustomer(xid, cid);
		boolean isRoomCustomerAdded = roomResourceManager.newCustomer(xid, cid);

		if(isFlightCustomerAdded && isCarCustomerAdded && isRoomCustomerAdded){
			Trace.info("Customer with xid: " + xid + ", cid: " + cid + " added and replicated across servers.");
			return cid;
		} else {
			flightResourceManager.deleteCustomer(xid, cid);
			carResourceManager.deleteCustomer(xid, cid);
			roomResourceManager.deleteCustomer(xid, cid);
			throw new IllegalArgumentException("Failed to replicate customer as atleast one distributed server failed to add customer.");
		}
	}

	public boolean newCustomer(int xid, int customerID) throws RemoteException
	{
		Trace.info("Request for AddRooms received from Client.");
		Trace.info("Replicating customer information on distributed servers...");

		boolean isFlightCustomerAdded = flightResourceManager.newCustomer(xid, customerID);
		boolean isCarCustomerAdded = carResourceManager.newCustomer(xid, customerID);
		boolean isRoomCustomerAdded = roomResourceManager.newCustomer(xid, customerID);

		if(isFlightCustomerAdded && isCarCustomerAdded && isRoomCustomerAdded){
			Trace.info("Customer with xid: " + xid + ", cid: " + customerID + " added and replicated across servers.");
			return true;
		} else {
			Trace.info("Failed to replicate customer as atleast one distributed server failed to add customer.");
			flightResourceManager.deleteCustomer(xid, customerID);
			carResourceManager.deleteCustomer(xid, customerID);
			roomResourceManager.deleteCustomer(xid, customerID);
			return false;
		}
	}

	public boolean deleteFlight(int xid, int flightNum) throws RemoteException
	{
		Trace.info("Request for DeleteFlight received from Client.");
        Trace.info("Forwarding request to Flight Server...");
        if (flightResourceManager.deleteFlight(xid, flightNum)) {
            Trace.info("Flight server completed request successfully!");
            return true;
		} else {
            Trace.info("Flight server failed to complete request.");
            return false;
        }
	}

	public boolean deleteCars(int xid, String location) throws RemoteException
	{
		Trace.info("Request for DeleteCars received from Client.");
        Trace.info("Forwarding request to Car Server...");
        if (carResourceManager.deleteCars(xid, location)) {
            Trace.info("Car server completed request successfully!");
            return true;
		} else {
            Trace.info("Car server failed to complete request.");
            return false;
        }
	}

	public boolean deleteRooms(int xid, String location) throws RemoteException
	{
		Trace.info("Request for DeleteRooms received from Client.");
        Trace.info("Forwarding request to Room Server...");
        if (roomResourceManager.deleteRooms(xid, location)) {
            Trace.info("Room server completed request successfully!");
            return true;
		} else {
            Trace.info("Room server failed to complete request.");
            return false;
        }
	}

	public boolean deleteCustomer(int xid, int customerID) throws RemoteException
	{
		Trace.info("Request for DeleteCustomer received from Client.");
		Trace.info("Deleting customer across distributed servers...");

		boolean isFlightCustomerDeleted = flightResourceManager.deleteCustomer(xid, customerID);
		boolean isCarCustomerDeleted = carResourceManager.deleteCustomer(xid, customerID);
		boolean isRoomCustomerDeleted = roomResourceManager.deleteCustomer(xid, customerID);

		if(isFlightCustomerDeleted){
			Trace.info("Customer deleted on Flight server");
		} else {
			Trace.info("Failed to delete customer on Flight server");
		}

		if(isCarCustomerDeleted){
			Trace.info("Customer deleted on Car server");
		} else{
			Trace.info("Failed to delete customer on Car server");
		}
		
		if(isRoomCustomerDeleted){
			Trace.info("Customer deleted on Room server");
		} else{
			Trace.info("Failed to delete customer on Room server");
		}

		return isFlightCustomerDeleted && isCarCustomerDeleted && isRoomCustomerDeleted;
	}

	public int queryFlight(int xid, int flightNum) throws RemoteException
	{
		Trace.info("Request for QueryFlight received from Client.");
        Trace.info("Forwarding request to Flight Server...");
		int value = flightResourceManager.queryFlight(xid, flightNum);
        Trace.info("Flight server response sending back to Client.");
		return value;
	}

	public int queryCars(int xid, String location) throws RemoteException
	{
		Trace.info("Request for QueryCars received from Client.");
        Trace.info("Forwarding request to Car Server...");
		int value = carResourceManager.queryCars(xid, location);
        Trace.info("Car server response sending back to Client.");
		return value;
	}

	public int queryRooms(int xid, String location) throws RemoteException
	{
		Trace.info("Request for QueryRooms received from Client.");
        Trace.info("Forwarding request to Room Server...");
		int value = roomResourceManager.queryRooms(xid, location);
        Trace.info("Room server response sending back to Client.");
		return value;
	}

	public String queryCustomerInfo(int xid, int customerID) throws RemoteException
	{
		Trace.info("Request for QueryCustomer received from Client.");
		Trace.info("Obtaining client information from across distributed servers...");

		String flightCustomerInfo = flightResourceManager.queryCustomerInfo(xid, customerID);
		String carCustomerInfo = carResourceManager.queryCustomerInfo(xid, customerID);
		String roomCustomerInfo = roomResourceManager.queryCustomerInfo(xid, customerID);

		Trace.info("Returning customer information to client.");

		String s = "Bill for customer " + customerID + "\n";

		if(flightCustomerInfo.equals("") && 
			carCustomerInfo.equals("") && 
			roomCustomerInfo.equals("")){
			return s;
		}

		if(!flightCustomerInfo.equals("")){
			s += flightCustomerInfo;
		} 
		
		if(!carCustomerInfo.equals("")){
			s += carCustomerInfo;
		}
		
		if(!roomCustomerInfo.equals("")){
			s+= roomCustomerInfo;
		}

		return s;
	}

	public int queryFlightPrice(int xid, int flightNum) throws RemoteException
	{
		Trace.info("Request for QueryFlightPrice received from Client.");
        Trace.info("Forwarding request to Flight Server...");
		int value = flightResourceManager.queryFlightPrice(xid, flightNum);
        Trace.info("Flight server response sending back to Client.");
		return value;
	}

	public int queryCarsPrice(int xid, String location) throws RemoteException
	{
		Trace.info("Request for QueryCarsPrice received from Client.");
        Trace.info("Forwarding request to Car Server...");
		int value = carResourceManager.queryCarsPrice(xid, location);
        Trace.info("Car server response sending back to Client.");
		return value;
	}

	public int queryRoomsPrice(int xid, String location) throws RemoteException
	{
		Trace.info("Request for QueryRoomsPrice received from Client.");
        Trace.info("Forwarding request to Room Server...");
		int value = roomResourceManager.queryRoomsPrice(xid, location);
        Trace.info("Room server response sending back to Client.");
		return value;
	}


	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException
	{
		Trace.info("Request for ReserveFlight received from Client.");
		Trace.info("Forwarding request to Flight Server...");

		if(flightResourceManager.reserveFlight(xid, customerID, flightNum)){
			Trace.info("Flight server completed request successfully!");
			return true;
		} else{
			Trace.info("Flight server failed to complete request.");
            return false;
		}
	}


	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException
	{
		Trace.info("Request for ReserveCar received from Client.");
		Trace.info("Forwarding request to Car Server...");

		if(carResourceManager.reserveCar(xid, customerID, location)){
			Trace.info("Car server completed request successfully!");
			return true;
		} else{
			Trace.info("Car server failed to complete request.");
            return false;
		}
	}

	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException
	{
		Trace.info("Request for ReserveRoom received from Client.");
		Trace.info("Forwarding request to Room Server...");

		if(roomResourceManager.reserveRoom(xid, customerID, location)){
			Trace.info("Room server completed request successfully!");
			return true;
		} else{
			Trace.info("Room server failed to complete request.");
            return false;
		}
	}

	public boolean bundle(int xid, int customerId, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException
	{
		int failureCount = 0;
		int expectedFailureCount = 0;

		for(String flightNumber : flightNumbers){
			expectedFailureCount++;
			int flightNum = Integer.parseInt(flightNumber);
			if(!flightResourceManager.reserveFlight(xid, customerId, flightNum)){
				failureCount++;
			}
		}

		if(car){
			expectedFailureCount++;
			if(!carResourceManager.reserveCar(xid, customerId, location)) {
				failureCount++;
			}
		}

		if(room){
			expectedFailureCount++;
			if(!roomResourceManager.reserveRoom(xid, customerId, location)){
				failureCount++;
			}
		}
		return !(failureCount==expectedFailureCount);
	}

	public String getName() throws RemoteException
	{
		return m_name;
	}

	@Override
	public void abortTransaction(int transactionID) throws RemoteException {
		System.out.println("Aborting the transaction with ID: " + transactionID);

		try{
			checkTransactionID(transactionID);
		}catch(Exception exp){
			System.out.println(exp.getLocalizedMessage());
		}

		if(middleware_tm.ifActiveOnTransactID(transactionID)){
			Transaction transact = middleware_tm.readActiveTransaction(transactionID);

			Set<String> resources = transact.getUsedRMs();

			if (resources.contains("Flight")){
				flightResourceManager.abortTransaction(transactionID);
			}
			if (resources.contains("Flight")){
				carResourceManager.abortTransaction(transactionID);
			}
			if (resources.contains("Flight")){
				roomResourceManager.abortTransaction(transactionID);
			}

			endTransaction(transactionID, false);
		}else{
			System.out.println("There is no valid Transaction with the ID " + transactionID);
			return;
		}
	}

	@Override
	public boolean commitTransaction(int transactionID) throws RemoteException {
		if(!middleware_tm.ifActiveOnTransactID(transactionID)){
			System.out.println("There is no valid Transaction with the ID " + transactionID);
			return false;
		}else{
			int id = transactionID;
			System.out.print("Commit transaction:" + transactionID);

			checkTransactionID(id);
			Transaction transact = middleware_tm.readActiveTransaction(transactionID);

			Set<String> resources = transact.getUsedRMs();

			Trace.info("Resource=" + resources);
			if (resources.contains("Flight")){
				flightResourceManager.commitTransaction(transactionID);
			}
			if (resources.contains("Flight")){
				carResourceManager.commitTransaction(transactionID);
			}
			if (resources.contains("Flight")){
				roomResourceManager.commitTransaction(transactionID);
			}

			if (resources.contains("Customer")) {
				RMHashMap map = transact.getTransactData();

				synchronized (m_data) {
					Set<String> keys = map.keySet();
					for (String key : keys) {
						System.out.print("Write:(" + key + "," + map.get(key) + ")");
						m_data.put(key, map.get(key));
					}
				}
			}

			endTransaction(transactionID, true);

			return true;
		}
	}

	@Override
	public boolean shutdown() throws RemoteException {
		flightResourceManager.shutdown();
		carResourceManager.shutdown();
		roomResourceManager.shutdown();

		new Thread() {
			@Override
			public void run() {
				System.out.print("Shutting down thread...");
				try {
					sleep(5000);
				} catch (InterruptedException e) {
					System.out.println(e.getLocalizedMessage());
				}
				System.out.println("Shutting down completed, goodbye!");
				System.exit(0);
			}

		}.start();

		return true;
	}

	@Override
	public int start() throws RemoteException {
		int transactionID  = middleware_tm.start();
		Trace.info("Starting the transaction with ID: " + transactionID);
		return transactionID;
	}

	@Override
	public void addTransaction(int transactionID) throws RemoteException {
		Trace.info("MiddlewareResourceManager::addTransaction(" + transactionID + ") called");
		if (!transactionManager.ifActiveOnTransactID(transactionID)) {
			Trace.info("Added transaction");
			transactionManager.writeActiveTransaction(transactionID, new Transaction(transactionID));
		}
	}

	private void checkTransactionID(int transactionID) {
		if(middleware_tm.readActiveTransaction(transactionID) != null){
			updateAllowedTime(transactionID);
			return;
		}
		Trace.info("ERROR: Transaction is not active!");

		Boolean checkTransactID = middleware_tm.hasPastTransaction(transactionID);
		if(checkTransactID == null){
			System.out.println("The transaction with ID: " + transactionID + " doesn't exist!");
		}else if(checkTransactID == true) {
			System.out.println("The transaction with ID: " + transactionID + " has already been committed");
		}else{
			System.out.println("The transaction with ID: " + transactionID + " has been aborted");
		}
	}


	private void endTransaction(int transactionID, boolean committed) {
		// Move to inactive transactions
		middleware_tm.writeActiveTransaction(transactionID, null);
		middleware_tm.writePastTransaction(transactionID, committed);

		// TODO: do unlocking here with the transaction id
	}

	//TODO: Add this under every query with the appropriate parameters
	public void addResourceManagerUsed(int xid, String resourceName) {

	}
}
 
