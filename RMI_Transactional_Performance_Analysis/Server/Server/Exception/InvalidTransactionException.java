package Server.Exception;

public class InvalidTransactionException extends Exception{

    public InvalidTransactionException(int xid, String message)
    {
        super("InvalidTransactionException > xid " + xid + ": " + message);
    }

}
