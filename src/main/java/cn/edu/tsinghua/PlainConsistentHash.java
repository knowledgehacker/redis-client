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
 *
 * Issues:
 * a. a redis cache here is not used as a cache backed by content server,
 * thus when a node is added/removed, we need to move the elements rehashed around nodes to make them available.
 * b. we need to detect node changes(added/removed).
 */
public class PlainConsistentHash implements ConsistentHash {
    private final HashAlgorithm _hashAlgorithm;
    private final int _replicas;
    private final TreeMap<Integer, String> _cachePoints; // map host's hash code to host id

    public PlainConsistentHash(HashAlgorithm hashAlgorithm, int replicas) {
        _hashAlgorithm = hashAlgorithm;
        _replicas = replicas;
        _cachePoints = new TreeMap<Integer, String>();
    }

    public void initHosts(Map<String, String> host2Ids) {
        for(Map.Entry<String, String> host2Id: host2Ids.entrySet())
            addHost(host2Id.getKey(), host2Id.getValue());
    }

    public void addHost(String host, String id) {
        System.out.println("host: " + host + " => ");
        for(int i = 0; i < _replicas; ++i) {
            int cachePoint = _hashAlgorithm.hash(host + i);
            _cachePoints.put(cachePoint, id);
            System.out.println("hash code: " + cachePoint + " -> id: " + id);
        }
    }

    public void removeHost(String host, String id) {
        for(int i = 0; i < _replicas; ++i) {
            int cachePoint = _hashAlgorithm.hash(host + i);
            _cachePoints.remove(cachePoint);
        }
    }

    public String getHost(String key) {
        return getHost(_hashAlgorithm.hash(key));
    }

    public String getHost(byte[] key) {
        return getHost(_hashAlgorithm.hash(key));
    }

    private final String getHost(int code) {
        if(_cachePoints.isEmpty())
            return null;

        if(!_cachePoints.containsKey(code)) {
            Map.Entry<Integer, String> cachePoint = _cachePoints.higherEntry(code);
            code = (cachePoint == null) ? _cachePoints.firstKey() : cachePoint.getKey();
        }

        return _cachePoints.get(code);
    }
}
