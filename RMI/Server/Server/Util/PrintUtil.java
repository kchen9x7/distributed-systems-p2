package Server.Util;

import Server.Common.Trace;
import Server.Enumeration.ResourceManagerEnum;
import Server.LockManager.TransactionLockObject;

public class PrintUtil {

    public void printValidation(int xid){
        Trace.info("xid " + xid + ": Validating transaction." );
    }

    public void printIntro(int xid, String requestName){
        Trace.info("xid " + xid + ": Request for " + requestName + " received from Client.");
    }

    public void printLockRequest(int xid, TransactionLockObject.LockType lockType){
        Trace.info("xid " + xid + ": Requesting " + lockType + ".");
    }

    public void printForwarding(int xid, ResourceManagerEnum resourceManager){
        Trace.info("xid " + xid + ": Forwarding request to " + resourceManager + " Server...");
    }

    public void printAbort(int xid){
        Trace.info("xid " + xid + ": Aborting transaction.");
    }

    public void printCommit(int xid){
        Trace.info("xid " + xid + ": Committing transaction.");
    }

    public void printStart(int xid){
        Trace.info("xid " + xid + ": Starting transaction.");
    }

}
