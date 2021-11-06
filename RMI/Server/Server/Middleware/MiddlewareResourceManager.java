package Server.Middleware;

import Server.Enumeration.ResourceManagerEnum;
import Server.Exception.InvalidTransactionException;
import Server.Common.*;
import Server.Exception.TransactionAbortedException;
import Server.Interface.*;

import java.util.*;
import java.rmi.RemoteException;

import Server.LockManager.DeadlockException;
import Server.LockManager.LockManager;
import Server.LockManager.TransactionLockObject.LockType;
import Server.TransactionManager.TransactionManager;
import Server.Common.Transaction;
import Server.Util.PrintUtil;

public class MiddlewareResourceManager implements IResourceManager
{
	// --- middleware ---
	protected String m_name = "";
	protected RMHashMap m_data = new RMHashMap();

	// --- resource managers ---
	private IRemoteResourceManager flightResourceManager = null;
	private IRemoteResourceManager carResourceManager = null;
	private IRemoteResourceManager roomResourceManager = null;

	// --- transactions ---
	private int TTL = 1000000; //time to live
	private TransactionManager transactionManager;
	private LockManager lockManager;

	// --- misc ---
	private PrintUtil p;

	public MiddlewareResourceManager(String p_name) {
		this.m_name = p_name;
		this.lockManager = new LockManager();
		this.p = new PrintUtil();
		TransactionManager tm = new TransactionManager(this, this.TTL);
		setTransactionManager(tm);
	}

	public void setTransactionManager(TransactionManager transactionManager){
		this.transactionManager = transactionManager;
	}

	protected void setFlightResourceManager(IRemoteResourceManager flightResourceManager){
		this.flightResourceManager = flightResourceManager;
	}

	protected void setCarResourceManager(IRemoteResourceManager carResourceManager){
		this.carResourceManager = carResourceManager;
	}

	protected void setRoomResourceManager(IRemoteResourceManager roomResourceManager){
		this.roomResourceManager = roomResourceManager;
	}
	// IMPLEMENTED
	// ***********
	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice)
			throws RemoteException, InvalidTransactionException, TransactionAbortedException {
		p.printIntro(xid, "addFlight");

		p.printValidation(xid);
        isValidTransaction(xid);

		p.printLockRequest(xid, LockType.LOCK_WRITE);
        requestLock(xid, LockType.LOCK_WRITE, Flight.getKey(flightNum));
        addResourceManager(xid, ResourceManagerEnum.Flight);

		p.printForwarding(xid, ResourceManagerEnum.Flight);
        if (flightResourceManager.addFlight(xid, flightNum, flightSeats, flightPrice)) {
            Trace.info("xid " + xid + ": Flight server completed request successfully!");
            return true;
		} else {
            Trace.info("xid " + xid + ": Flight server failed to complete request.");
            return false;
        }
	}

	public boolean addCars(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException {
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

	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException {
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

	public int newCustomer(int xid) throws RemoteException, InvalidTransactionException {
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

	public boolean newCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException {
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

	public boolean deleteFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException {
		p.printIntro(xid, "deleteFlight");

		p.printValidation(xid);
		isValidTransaction(xid);

		p.printLockRequest(xid, LockType.LOCK_WRITE);
		requestLock(xid, LockType.LOCK_READ, Flight.getKey(flightNum));
		addResourceManager(xid, ResourceManagerEnum.Flight);

		p.printForwarding(xid, ResourceManagerEnum.Flight);
        if (flightResourceManager.deleteFlight(xid, flightNum)) {
            Trace.info("Flight server completed request successfully!");
            return true;
		} else {
            Trace.info("Flight server failed to complete request.");
            return false;
        }
	}

	public boolean deleteCars(int xid, String location) throws RemoteException, InvalidTransactionException {
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

	public boolean deleteRooms(int xid, String location) throws RemoteException, InvalidTransactionException {
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

	public boolean deleteCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException {
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

	// IMPLEMENTED
	// ***********
	public int queryFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException {
		p.printIntro(xid, "queryFlight");

		p.printValidation(xid);
		isValidTransaction(xid);

		p.printLockRequest(xid, LockType.LOCK_READ);
		requestLock(xid, LockType.LOCK_READ, Flight.getKey(flightNum));
		addResourceManager(xid, ResourceManagerEnum.Flight);

		p.printForwarding(xid, ResourceManagerEnum.Flight);
		int value = flightResourceManager.queryFlight(xid, flightNum);

		Trace.info("xid " + xid + ": Flight server response sending back to Client.");
		return value;
	}

	public int queryCars(int xid, String location) throws RemoteException, InvalidTransactionException {
		Trace.info("Request for QueryCars received from Client.");
        Trace.info("Forwarding request to Car Server...");
		int value = carResourceManager.queryCars(xid, location);
        Trace.info("Car server response sending back to Client.");
		return value;
	}

	public int queryRooms(int xid, String location) throws RemoteException, InvalidTransactionException {
		Trace.info("Request for QueryRooms received from Client.");
        Trace.info("Forwarding request to Room Server...");
		int value = roomResourceManager.queryRooms(xid, location);
        Trace.info("Room server response sending back to Client.");
		return value;
	}

	public String queryCustomerInfo(int xid, int customerID) throws RemoteException, InvalidTransactionException {
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

	public int queryFlightPrice(int xid, int flightNum) throws RemoteException, InvalidTransactionException {
		Trace.info("Request for QueryFlightPrice received from Client.");
        Trace.info("Forwarding request to Flight Server...");
		int value = flightResourceManager.queryFlightPrice(xid, flightNum);
        Trace.info("Flight server response sending back to Client.");
		return value;
	}

	public int queryCarsPrice(int xid, String location) throws RemoteException, InvalidTransactionException {
		Trace.info("Request for QueryCarsPrice received from Client.");
        Trace.info("Forwarding request to Car Server...");
		int value = carResourceManager.queryCarsPrice(xid, location);
        Trace.info("Car server response sending back to Client.");
		return value;
	}

	public int queryRoomsPrice(int xid, String location) throws RemoteException, InvalidTransactionException {
		Trace.info("Request for QueryRoomsPrice received from Client.");
        Trace.info("Forwarding request to Room Server...");
		int value = roomResourceManager.queryRoomsPrice(xid, location);
        Trace.info("Room server response sending back to Client.");
		return value;
	}


	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, InvalidTransactionException {
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


	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException {
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

	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException {
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

	public boolean bundle(int xid, int customerId, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException, InvalidTransactionException {
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


	public int start() throws RemoteException {
		int xid  = transactionManager.initiateTransaction();
		p.printStart(xid);
		return xid;
	}

	public void abort(int xid) throws RemoteException, InvalidTransactionException {
		p.printAbort(xid);

		isValidTransaction(xid);

		Transaction t = transactionManager.getOngoingTransaction(xid);
		Set<ResourceManagerEnum> resourceManagers = t.getInteractedResourceManagers();

		if (resourceManagers.contains(ResourceManagerEnum.Flight)) {
			flightResourceManager.abort(xid);
		}
		if (resourceManagers.contains(ResourceManagerEnum.Car)){
			carResourceManager.abort(xid);
		}
		if (resourceManagers.contains(ResourceManagerEnum.Room)){
			roomResourceManager.abort(xid);
		}

		transactionManager.nullifyOngoingTransaction(xid);

		t.setCommitted(false);
		transactionManager.addToDeadTransactions(t);

		lockManager.UnlockAll(xid);
	}

	private boolean isValidTransaction(int xid) throws InvalidTransactionException {
		if (this.transactionManager.isOngoingTransaction(xid)){
			this.transactionManager.resetLatestInteraction(xid);
			return true;
		}

		throw new InvalidTransactionException(xid, "The transaction has either been completed, has been aborted or does not exist.");
	}
	// AAA
	protected void requestLock(int xid, LockType lockType, String data)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException{
		try {
			boolean isLockAcquired = lockManager.Lock(xid, data, lockType);
			if (!isLockAcquired) {
				Trace.info("LockManager::Lock(" + xid + ", " + lockType + ", " + data + ") Unable to acquire requested lock");
				throw new InvalidTransactionException(xid, "LockManager unable to acquire requested " + lockType + " lock for transaction");
			}
		} catch (DeadlockException e) {
			Trace.info("LockManager::Lock(" + xid + ", " + lockType + ", " + data + ") " + e.getMessage());
			abort(xid);
			throw new TransactionAbortedException(xid, "The transaction was aborted due a deadlock");
		}
	}

	protected void addResourceManager(int xid, ResourceManagerEnum resourceManager) throws RemoteException {
		Transaction t = transactionManager.getOngoingTransaction(xid);
		t.addInteractedResourceManager(resourceManager);

		if(resourceManager.equals(ResourceManagerEnum.Flight)){
			flightResourceManager.addTransaction(xid);
		}
		if(resourceManager.equals(ResourceManagerEnum.Car)){
			carResourceManager.addTransaction(xid);
		}
		if(resourceManager.equals(ResourceManagerEnum.Room)){
			roomResourceManager.addTransaction(xid);
		}
	}

	public boolean commit(int xid)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		p.printCommit(xid);

		isValidTransaction(xid);

		Transaction t = transactionManager.getOngoingTransaction(xid);

		Set<ResourceManagerEnum> resourceManagers = t.getInteractedResourceManagers();
		Trace.info("ResourceManagers for xid " + xid + " : " + resourceManagers.toString());

		if (resourceManagers.contains(ResourceManagerEnum.Flight)) {
			flightResourceManager.commit(xid);
		}
		if (resourceManagers.contains(ResourceManagerEnum.Car)){
			carResourceManager.commit(xid);
		}
		if (resourceManagers.contains(ResourceManagerEnum.Room)){
			roomResourceManager.commit(xid);
		}

		transactionManager.nullifyOngoingTransaction(xid);
		t.setCommitted(true);
		transactionManager.addToDeadTransactions(t);
		lockManager.UnlockAll(xid);

		return true;
	}

}
 
