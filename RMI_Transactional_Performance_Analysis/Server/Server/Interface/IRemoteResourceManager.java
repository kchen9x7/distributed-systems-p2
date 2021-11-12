package Server.Interface;

import java.rmi.RemoteException;

public interface IRemoteResourceManager extends IResourceManager {

    /**
     * Add the transaction to the remote Resource manager's local TransactionManager
     */
    public void addTransaction(int xid) throws RemoteException;

}
