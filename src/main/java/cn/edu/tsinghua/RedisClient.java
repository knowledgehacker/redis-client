package cn.edu.tsinghua;

/**
 * Created by mlin on 9/26/14.
 */
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

/*
 * Here we create a JedisPool for master node, actually only one Jedis instance in the JedisPool is used.
 * As to slave nodes, one Jedis instance per slave node is created, we don't use JedisPool.
 * For a batch read, we deliver keys to slave nodes in parallel.
 * For a batch write, we deliver keys to the master node.
 * Here we don't deliver keys to master node for read operations, it is responsible for write operations.
 *
 * Question: Do we need sharding on slave nodes?
 * With sharding, only a partition of keys in the master node are replicated to a slave node?
 * If we use sharding, and one salve node only contains a partition of keys, then what will happen
 * once we deliver some keys to a slave node for a read operation, and these keys are not in this slave node?
 *
 * TODO: handle the case that the master node goes down.
 */
public class RedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    private static final int DEFAULT_TIMEOUT = 3000;

    private static JedisPool _masterJedisPool;
    private Jedis _masterJedis;
    private List<Jedis> _slaveJedisList;

    public RedisClient(String masterHost, List<String> slaveHosts) {
        HostnameAndPort master = new HostnameAndPort(masterHost);
        String masterHostName = master.getHostName();
        int masterHostPort = master.getPort();
        _masterJedisPool = new JedisPool(new JedisPoolConfig(), masterHostName, masterHostPort, DEFAULT_TIMEOUT);
        _masterJedis = _masterJedisPool.getResource();

        _slaveJedisList = new ArrayList<Jedis>();
        for(String slaveHost: slaveHosts) {
	    HostnameAndPort slave = new HostnameAndPort(slaveHost);
            Jedis slaveJedis = new Jedis(slave.getHostName(), slave.getPort(), DEFAULT_TIMEOUT);
            slaveJedis.slaveof(masterHostName, masterHostPort);

            _slaveJedisList.add(slaveJedis);
        }
    }

    public final byte[] get(byte[] key) {
        return _masterJedis.get(key);
    }

    public final String get(String key) {
        return _masterJedis.get(key);
    }

    public final void set(byte[] key, byte[] value, int seconds) {
        _masterJedis.setex(key, seconds, value);
    }

    public final void set(String key, String value, int seconds) {
        _masterJedis.setex(key, seconds, value);
    }

    public List<String> multiGet(List<String> keys) {
        List<String> values = new ArrayList<String>();

        int partitionSize = keys.size() / _slaveJedisList.size();
        int extraSize = keys.size() % _slaveJedisList.size();
        if(extraSize != 0) {
            // deliver "partitionSize+1" keys to the first "extraSize" slave Jediss
            getPartitionedValues(0, extraSize, partitionSize+1, keys, values);
            // deliver "partitionSize" keys to the last slave Jediss
            getPartitionedValues(extraSize, _slaveJedisList.size(), partitionSize, keys, values);
        } else
            getPartitionedValues(0, _slaveJedisList.size(), partitionSize, keys, values);

        return values;
    }

    private final void getPartitionedValues(int start, int end, int partitionSize, List<String> keys, List<String> values) {
        String[] partitionedKeys = new String[partitionSize];
        for (int i = start; i < end; ++i) {
            for(int j = i*partitionSize, k = 0; j < (i+1)*partitionSize; ++j, ++k)
                partitionedKeys[k] = keys.get(j);
            values.addAll(_slaveJedisList.get(i).mget(partitionedKeys));
        }
    }

    /*
     * From test cases in the following file, we how to pass <key, value> pairs to Jedis.mset method.
     * https://github.com/xetorthio/jedis/blob/master/src/test/java/redis/clients/jedis/tests/commands/StringValuesCommandsTest.java
     *
     * We can't set expire time for keys for batch write operation???
     */
    public void multiSet(List<String> keys, List<String> values) {
        int keySize = keys.size();
        if(keySize != values.size()) {
            LOG.error("keys.size() != values.size()");
            return;
        }

        String[] keyValues = new String[keys.size()*2];
        for(int i = 0; i < keys.size(); ++i) {
            keyValues[2 * i] = keys.get(i);
            keyValues[2 * i + 1] = values.get(i);
        }

        _masterJedis.mset(keyValues);
    }

    public void close() {
        _masterJedis.close();
        _masterJedisPool.close();

        for(Jedis slaveJedis: _slaveJedisList)
            slaveJedis.close();
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

        RedisClient client = new RedisClient(masterHost, slaveHosts);

        long start = System.currentTimeMillis();
        List<String> keys = new ArrayList<String>();
        for(int i = 0; i < 10000; ++i)
            keys.add("test_key_" + i);
        List<String> values = new ArrayList<String>();
        for(int i = 0; i < 10000; ++i)
            values.add("test_value_" + i);
        client.multiSet(keys, values);

        List<String> subValues = client.multiGet(keys.subList(10, 20));
        for(String value: subValues)
            System.out.println("value: " + value);
        System.out.println("Time took " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}
