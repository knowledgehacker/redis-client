package cn.edu.tsinghua;

import java.util.Map;

/**
 * Created by mlin on 9/30/14.
 */
public interface ConsistentHashing {
    public void initHosts(Map<String, String> host2Ids);
    public void addHost(String host, String id);
    public void removeHost(String host, String id);

    // return the id of node, which is represented in String
    String hash(String key);
    String hash(byte[] key);
}
