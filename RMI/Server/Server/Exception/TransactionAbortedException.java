package Server.Exception;

public class TransactionAbortedException extends Exception{

    public TransactionAbortedException(int xid, String message)
    {
        super("Transaction " + xid + ":" + message);
    }

}
