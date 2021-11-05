package Server.Transactions;
import Server.Middleware.*;
import java.util.Set;

public class MiddlewareTransactionHandler extends TransactionManager implements Runnable{
    private Integer transactionID;
    private int time_allowed;
    private MiddlewareResourceManager middleware_rm;

    public MiddlewareTransactionHandler(int time_allowed, MiddlewareResourceManager rm){
        super();
        this.time_allowed = time_allowed;
        this.middleware_rm = rm;
        new Thread(this).start();
    }

    public synchronized int start(){
        this.transactionID++;
        int id = this.transactionID;
        this.writeActiveTransaction(id, new Transaction(id, this.time_allowed));
        return id;
    }

    @Override
    public void run() {
        while(true){
            try{
                synchronized (activeTransactions){
                    Set<Integer> keys = activeTransactions.keySet();

                    for(Integer key : keys){
                        Transaction transaction = activeTransactions.get(key);

                        if(transaction.exceedTime() && transaction != null){
                            this.middleware_rm.abortTransaction(transaction.getTransactionID());
                        }
                    }
                }

                Thread.sleep(5000);
            }catch(Exception exp){
                System.out.println(exp.getLocalizedMessage());
            }
        }
    }
}
