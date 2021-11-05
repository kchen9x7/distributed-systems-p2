package Server.Transactions;
import java.util.HashMap;
import java.util.Set;

public class TransactionManager {
    protected HashMap<Integer, Transaction> activeTransactions = new HashMap<Integer, Transaction>();
    //When the past transaction is aborted then it's false, otherwise it's true
    protected HashMap<Integer, Boolean> pastTransactions = new HashMap<Integer, Boolean>();

    public TransactionManager(){}

    public Transaction readActiveTransaction(int id){
        synchronized (activeTransactions){
            return activeTransactions.get(id);
        }
    }

    public void writeActiveTransaction(int transactionID, Transaction transact){
        synchronized (activeTransactions){
            activeTransactions.put(transactionID, transact);
        }
    }

    public boolean hasPastTransaction(int transactionID){
        synchronized (pastTransactions){
            return (pastTransactions.get(transactionID));
        }
    }

    public void writePastTransaction(int transactionID, Boolean ifCommitted){
        synchronized (pastTransactions){
            pastTransactions.put(transactionID, ifCommitted);
        }
    }


    public boolean ifActiveOnTransactID(int transactionID){
        synchronized (activeTransactions){
            synchronized (pastTransactions){
                Set<Integer> activeTransactKeys = activeTransactions.keySet();
                Set<Integer> pastTransactKeys = pastTransactions.keySet();
                return (!pastTransactKeys.contains(transactionID) && activeTransactKeys.contains(transactionID));
            }
        }
    }




}
