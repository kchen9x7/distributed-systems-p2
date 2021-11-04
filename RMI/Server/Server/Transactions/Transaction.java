package Server.Transactions;
import Server.Common.*;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;


public class Transaction {
    protected int transactionID;
    protected RMHashMap transacDataMap = new RMHashMap();
    private int time_allowed; //in seconds
    private long endTime = (new Date()).getTime(); //in milliseconds
    private HashSet<String> rm_used = new HashSet<String>();

    //constructors
    public Transaction(int id){this.transactionID = id;}
    public Transaction(int id, int time_allowed) {
        this.transactionID = id;
        this.time_allowed = time_allowed * 1000; //seconds converted to milliseconds
    }

    public RMHashMap getTransacData(){return this.transacDataMap;}
    public HashSet<String> getUsedRMs(){return this.rm_used;}
    public int getTransactionID(){return this.transactionID;}

    public void addUsedRMs(String resourceManager){
        rm_used.add(resourceManager);
        System.out.print("Added resource manager " + resourceManager + " in this transaction.");
    }

    public void write(int transactionId, String key, RMItem item){
        synchronized (transacDataMap)  {
            transacDataMap.put(key, item);
        }
    }

    public RMItem read(int id, String key){
        synchronized (transacDataMap){
            RMItem item = transacDataMap.get(key);
            if (item != null) {
                return (RMItem)item.clone();
            }
        }
        return null;
    }

    public boolean hasTransactionData(String itemKey){
        synchronized (transacDataMap){
            Set<String> keys = transacDataMap.keySet();
            return keys.contains(itemKey);
        }
    }

    public void updateLastOperationTime(){
        this.endTime = (new Date()).getTime();
        System.out.println("The end time of the last operation is " + endTime);
    }

    public boolean exceedTime(){
        long currTime = (new Date()).getTime(); //in milliseconds
        System.out.println("The current time in milliseconds for this operation is " + currTime + ", and the end time of the last operation is " + endTime);

        if(time_allowed <= currTime + endTime){
            return false;
        }else{
            return true;
        }
    }

}
