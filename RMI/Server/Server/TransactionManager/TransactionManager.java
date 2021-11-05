package Server.TransactionManager;

import Server.Common.Transaction;
import Server.Exception.InvalidTransactionException;
import Server.Common.Trace;
import Server.Middleware.MiddlewareResourceManager;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;

public class TransactionManager implements Runnable, Serializable {

    private int xidCount = 0;
    private int TTL; // in seconds
    private HashMap<Integer, Transaction> ongoingTransactions;
    private HashMap<Integer, Transaction> deadTransactions;

    private MiddlewareResourceManager middleware;

    public TransactionManager(){
        this.ongoingTransactions = new HashMap<>();
        this.deadTransactions = new HashMap<>();
    }

    public TransactionManager(MiddlewareResourceManager middleware, int TTL){
        this();
        this.TTL = TTL;
        (new Thread(this)).start();
        this.middleware = middleware;
    }

    public synchronized int initiateTransaction(){
        this.xidCount++;
        int xid = this.xidCount;
        Transaction t = new Transaction(xid, this.TTL);
        addToOngoingTransactions(t);
        return xid;
    }

    public void addToOngoingTransactions(Transaction t){
        synchronized (ongoingTransactions){
            this.ongoingTransactions.put(t.getTransactionId(), t);
        }
    }

    public void addToDeadTransactions(Transaction t){
        synchronized (deadTransactions){
            this.deadTransactions.put(t.getTransactionId(), t);
        }
    }

    public HashMap<Integer, Transaction> getOngoingTransactions(){
        return this.ongoingTransactions;
    }

    public Transaction getOngoingTransaction(int xid){
        return this.ongoingTransactions.get(xid);
    }

    public void nullifyOngoingTransaction(int xid){
        this.ongoingTransactions.put(xid, null);
    }

    public HashMap<Integer, Transaction> getDeadTransactions(){
        return this.deadTransactions;
    }

    public boolean resetLatestInteraction(int xid){
        return getOngoingTransactions().get(xid).resetLatestInteraction();
    }

    public void run() {
        while(true){
            synchronized (ongoingTransactions){
                HashMap<Integer, Transaction> ongoingTrans = getOngoingTransactions();
                for(Integer key : ongoingTrans.keySet()){
                    Transaction t = ongoingTrans.get(key);

                    if(t != null && t.isExpired()){
                        Trace.info("Transaction with xid: " + t.getTransactionId() + " has expired. Now aborting...");
                        try {
                            this.middleware.abort(t.getTransactionId());
                        } catch (RemoteException | InvalidTransactionException e) {
                            Trace.info(e.getMessage());
                        }
                    }
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Trace.info(e.getMessage());
            }
        }
    }

    public boolean isOngoingTransaction(int xid){
        return this.ongoingTransactions.get(xid) != null;
    }

}
