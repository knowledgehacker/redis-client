package cn.edu.tsinghua;

import java.util.Map;

/**
 * Created by mlin on 9/30/14.
 */
public interface ConsistentHash {
    void initHosts(Map<String, String> host2Ids);
    void addHost(String host, String id);
    void removeHost(String host, String id);

    String getHost(String key);
    String getHost(byte[] key);
}
