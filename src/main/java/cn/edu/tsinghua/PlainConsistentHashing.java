package cn.edu.tsinghua;

/**
 * Created by mlin on 9/30/14.
 */
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/*
 * Do we need to synchronize operations on "_cachePoints"?
 */
public class PlainConsistentHashing implements ConsistentHashing {
    private static final int INTERVAL_NUM = 167;

    private TreeMap<Integer, String> _cachePoints; // map host's hash code to host id

    public PlainConsistentHashing() {
    }

    public void initHosts(Map<String, String> host2Ids) {
        _cachePoints = new TreeMap<Integer, String>();
        for(Map.Entry<String, String> host2Id: host2Ids.entrySet()) {
            int cachePoint = host2Id.getKey().hashCode() % INTERVAL_NUM;
            _cachePoints.put(cachePoint, host2Id.getValue());
            System.out.println("host: " + host2Id.getKey() + " => hash code: " + cachePoint + " => id: " + host2Id.getValue());
        }
    }

    public void addHost(String host, String id) {
        int cachePoint = host.hashCode() % INTERVAL_NUM;
        _cachePoints.put(cachePoint, id);
        System.out.println("host: " + host + " => hash code: " + cachePoint + " => id: " + id);
    }

    public void removeHost(String host, String id) {
        int cachePoint = host.hashCode() % INTERVAL_NUM;
        _cachePoints.remove(cachePoint);
    }

    public String hash(String key) {
        return hashKey(key.hashCode() % INTERVAL_NUM);
    }

    public String hash(byte[] key) {
        return hashKey(key.hashCode() % INTERVAL_NUM);
    }

    private final String hashKey(int code) {
        Map.Entry<Integer, String> cachePoint = _cachePoints.higherEntry(code);

        return (cachePoint == null) ? _cachePoints.firstEntry().getValue() : cachePoint.getValue();
    }
}