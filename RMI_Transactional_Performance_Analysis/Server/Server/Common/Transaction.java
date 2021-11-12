package Server.Common;

import Server.Enumeration.ResourceManagerEnum;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Transaction {

    private int xid;
    private int TTL; // in millseconds
    private Set<ResourceManagerEnum> interactedResourceManagers;
    private long latestInteraction;
    private boolean isCommitted;
    private RMHashMap m_data = new RMHashMap();

    public Transaction(int xid, int TTL){
        this.xid = xid;
        this.TTL = TTL;
        this.latestInteraction = (new Date()).getTime();
        this.interactedResourceManagers = new HashSet<>();
        this.isCommitted = false;
    }

    public Transaction(int xid){
        this.xid = xid;
    }

    public int getTransactionId(){
        return this.xid;
    }

    public boolean isExpired(){
        Date date = new Date();
        long currentTime = date.getTime();
        return isTimeExceeded(currentTime);
    }

    public boolean resetLatestInteraction(){
        this.latestInteraction = (new Date()).getTime();
        return true;
    }

    private boolean isTimeExceeded(long currentTime){
        if(currentTime > this.latestInteraction + this.TTL) {
            return true;
        }
        return false;
    }

    public Set<ResourceManagerEnum> getInteractedResourceManagers() {
        return this.interactedResourceManagers;
    }

    public void setCommitted(boolean isCommitted){
        this.isCommitted = isCommitted;
    }

    public void addInteractedResourceManager(ResourceManagerEnum resourceManager){
        this.interactedResourceManagers.add(resourceManager);
    }

    public void writeData(String key, RMItem value) {
        synchronized (m_data){
            m_data.put(key, value);
        }
    }

    public RMItem readData(String key){
        synchronized(m_data) {
            RMItem item = m_data.get(key);
            if (item != null) {
                return (RMItem)item.clone();
            }
            return null;
        }
    }

    public RMHashMap getData() {
        return m_data;
    }
}
