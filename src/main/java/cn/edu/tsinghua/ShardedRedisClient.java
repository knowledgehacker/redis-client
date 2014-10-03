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
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.util.Hashing;

public class ShardedRedisClient {
    protected static final Logger LOG = LoggerFactory.getLogger(ShardedRedisClient.class);

    private static final int DEFAULT_TIMEOUT = 3000;

    private final JedisPool _masterJedisPool;
    private final Jedis _masterJedis;
    private final ShardedJedis _slaveShardedJedis;

    public ShardedRedisClient(String masterHost, List<String> slaveHosts) {
        this(masterHost, slaveHosts, DEFAULT_TIMEOUT);
    }

    public ShardedRedisClient(String masterHost, List<String> slaveHosts, int timeout) {
        HostnameAndPort master = new HostnameAndPort(masterHost);
        String masterHostName = master.getHostName();
        int masterHostPort = master.getPort();
        _masterJedisPool = new JedisPool(new JedisPoolConfig(), masterHostName, masterHostPort, timeout);
        _masterJedis = _masterJedisPool.getResource();

        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        HostnameAndPort slave = null;
        for(String slaveHost: slaveHosts) {
            slave = new HostnameAndPort(slaveHost);
            shards.add(new JedisShardInfo(slave.getHostName(), slave.getPort(), timeout));
        }
        _slaveShardedJedis = new ShardedJedis(shards, Hashing.MURMUR_HASH); // use murmur hash to hash the keys

        for(Jedis slaveJedis: _slaveShardedJedis.getAllShards())
            slaveJedis.slaveof(masterHostName, masterHostPort);
    }

    public final String get(String key) {
        return _slaveShardedJedis.get(key);
    }

    public final void set(String key, String value, int seconds) {
        _slaveShardedJedis.setex(key, seconds, value);
    }

    public final byte[] get(byte[] key) {
        return _slaveShardedJedis.get(key);
    }

    public final void set(byte[] key, byte[] value, int seconds) {
        _slaveShardedJedis.setex(key, seconds, value);
    }

    /*
     * batch get/set operations on String.
     */
    public final List<String> hMultiGet(String shardKey, List<String> fields) {
        return _slaveShardedJedis.hmget(shardKey, fields.toArray(new String[fields.size()]));
    }

    public final void hMultiSet(String shardKey, Map<String, String> fieldValues, int seconds) {
        Pipeline pipeline = _masterJedis.pipelined();
        pipeline.hmset(shardKey, fieldValues);
        pipeline.expire(shardKey, seconds);
        pipeline.sync();
    }

    public final List<String> multiGet(List<String> keys) {
        List<String> values = new ArrayList<String>();

        ShardedJedisPipeline pipeline = _slaveShardedJedis.pipelined();
        for(String key: keys)
            pipeline.get(key);
        for(Object value: pipeline.syncAndReturnAll())
            values.add((String)value);

        return values;
    }

    public final void multiSet(Map<String, String> keyValues, int seconds) {
        Pipeline pipeline = _masterJedis.pipelined();
        for(Map.Entry<String, String> keyValue: keyValues.entrySet())
            pipeline.setex(keyValue.getKey(), seconds, keyValue.getValue());
        pipeline.sync();
    }

    public final void multiSet(List<String> keys, List<String> values, int seconds) {
        int keySize = keys.size();
        if(keySize != values.size()) {
            LOG.error("keys.size() != values.size()");
            return;
        }

        Pipeline pipeline = _masterJedis.pipelined();
        for(int i = 0; i < keySize; ++i)
            pipeline.setex(keys.get(i), seconds, values.get(i));
        pipeline.sync();
    }

    /*
     * batch get/set operations on byte[].
     */
    public final List<byte[]> hBMultiGet(byte[] shardKey, List<byte[]> fields) {
        return _slaveShardedJedis.hmget(shardKey, fields.toArray(new byte[fields.size()][]));
    }

    public final void hBMultiSet(byte[] shardKey, Map<byte[], byte[]> fieldValues, int seconds) {
        Pipeline pipeline = _masterJedis.pipelined();
        pipeline.hmset(shardKey, fieldValues);
        pipeline.expire(shardKey, seconds);
        pipeline.sync();
    }

    public final List<byte[]> bMultiGet(List<byte[]> keys) {
        List<byte[]> values = new ArrayList<byte[]>();

        ShardedJedisPipeline pipeline = _slaveShardedJedis.pipelined();
        for(byte[] key: keys)
            pipeline.get(key);
        for(Object value: pipeline.syncAndReturnAll())
            values.add((byte[])value);

        return values;
    }

    public final void bMultiSet(Map<byte[], byte[]> keyValues, int seconds) {
        Pipeline pipeline = _masterJedis.pipelined();
        for(Map.Entry<byte[], byte[]> keyValue: keyValues.entrySet())
            pipeline.setex(keyValue.getKey(), seconds, keyValue.getValue());
        pipeline.sync();
    }

    public final void bMultiSet(List<byte[]> keys, List<byte[]> values, int seconds) {
        int keySize = keys.size();
        if(keySize != values.size()) {
            LOG.error("keys.size() != values.size()");
            return;
        }

        Pipeline pipeline = _masterJedis.pipelined();
        for(int i = 0; i < keySize; ++i)
            pipeline.setex(keys.get(i), seconds, values.get(i));
        pipeline.sync();
    }

    /*
    public final void multiSet_xx(Map<String, String> keyValues, int seconds) {
        Pipeline pipeline = _masterJedis.pipelined();

        String[] kvs = new String[keyValues.size()*2];
        int i = 0;
        for(Map.Entry<String, String> keyValue: keyValues.entrySet()) {
            kvs[2*i] = keyValue.getKey();
            kvs[2*i+1] = keyValue.getValue();
            ++i;
        }
        pipeline.mset(kvs);
        for(i = 0; i < keyValues.size(); ++i)
            pipeline.expire(kvs[2*i], seconds);

        pipeline.sync();
    }
    */

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
            fieldValues.put(fields.get(i), "test_value_multiSet_xx" + i);
        client.multiSet(fieldValues, 3);

        List<String> values = client.multiGet(fields.subList(0, 10));
        for(String value: values)
            System.out.println("value: " + value);
        try {
            System.out.println("Sleep 1 seconds ...");
            Thread.sleep(1 * 1000);
        } catch(InterruptedException ie) {
                LOG.warn(ie.toString());
        }

        values = client.multiGet(fields.subList(0, 10));
        for(String value: values)
            System.out.println("value: " + value);

        System.out.println("Time took " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}