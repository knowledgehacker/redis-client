package cn.edu.tsinghua;

/**
 * Created by mlin on 9/26/14.
 */
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;

public class ShardedRedisClient {
    protected static final Logger LOG = LoggerFactory.getLogger(ShardedRedisClient.class);

    private static final int DEFAULT_TIMEOUT = 3000;
    private static final String LOG_RECORD_SHARD_KEY = "log_record:";

    private final JedisPool _masterJedisPool;
    private final Jedis _masterJedis;
    private final ShardedJedis _slaveShardedJedis;

    public ShardedRedisClient(String masterHost, List<String> slaveHosts) {
        HostnameAndPort master = new HostnameAndPort(masterHost);
        String masterHostName = master.getHostName();
        int masterHostPort = master.getPort();
        _masterJedisPool = new JedisPool(new JedisPoolConfig(), masterHostName, masterHostPort, DEFAULT_TIMEOUT);
        _masterJedis = _masterJedisPool.getResource();

        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        HostnameAndPort slave = null;
        for(String slaveHost: slaveHosts) {
            slave = new HostnameAndPort(slaveHost);
            shards.add(new JedisShardInfo(slave.getHostName(), slave.getPort(), 3000));
        }
        _slaveShardedJedis = new ShardedJedis(shards, Hashing.MURMUR_HASH); // use murmur hash to hash the keys

        for(Jedis slaveJedis: _slaveShardedJedis.getAllShards())
            slaveJedis.slaveof(masterHostName, masterHostPort);
    }

    public final List<String> multiGet(List<String> fields) {
        return _slaveShardedJedis.hmget(LOG_RECORD_SHARD_KEY, fields.toArray(new String[fields.size()]));
    }

    public final void multiSet(Map<String, String> fieldValues) {
        _masterJedis.hmset(LOG_RECORD_SHARD_KEY, fieldValues);
    }

    public void close() {
        _masterJedis.close();
        _masterJedisPool.close();

        _slaveShardedJedis.close();
    }

    private static class HostnameAndPort {
        private final String _hostName;
        private final int _port;

        public HostnameAndPort(String host) {
            int idx = host.indexOf(':');
            _hostName = host.substring(0, idx);
            _port = Integer.parseInt(host.substring(idx+1));
        }

        public final String getHostName() {
            return _hostName;
        }

        public final int getPort() {
            return _port;
        }
    }

    public static void main(String[] args) {
        String masterHost = "127.0.0.1:7000";
        List<String> slaveHosts = new ArrayList<String>();
        slaveHosts.add("127.0.0.1:7001");
        slaveHosts.add("127.0.0.1:7002");

        ShardedRedisClient client = new ShardedRedisClient(masterHost, slaveHosts);
       
	long start = System.currentTimeMillis();
        List<String> fields = new ArrayList<String>();
        for(int i = 0; i < 10000; ++i)
            fields.add("test_field_" + i);
        Map<String, String> fieldValues = new HashMap<String, String>();
        for(int i = 0; i < fields.size(); ++i)
            fieldValues.put(fields.get(i), "test_value_" + i);
        client.multiSet(fieldValues);

        List<String> values = client.multiGet(fields.subList(10, 20));
        for(String value: values)
            System.out.println("value: " + value);
        System.out.println("Time took " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}
