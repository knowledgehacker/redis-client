package cn.edu.tsinghua;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONObject;
import org.json.JSONArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.util.ConfigLoader;

public class Sharding {
	private static final Logger LOG = LoggerFactory.getLogger(Sharding.class);

	private static final String DEFAULT_REDIS_CONFIG_FILE = "/redis.json";

	private static final String REDIS_SETUP_KEY = "redis.setup";
	private static final String MASTER_ID_KEY = "master.id";
	private static final String MASTER_HOST_KEY = "master.host";
	private static final String SLAVE_HOSTS_KEY = "slave.hosts";

    private final ConsistentHash _hashing;
    private Map<String, ShardedRedisClient> _shardedRedisClients;

	public Sharding(ConsistentHash hashing) {
        _hashing = hashing;
        _shardedRedisClients = new HashMap<String, ShardedRedisClient>();
	}

    public void init() throws Exception {
        init(null);
    }

    public void init(String configFile) throws Exception {
        Map<String, String> host2Ids = new HashMap<String, String>();
        try {
            String config = (configFile == null) ? ConfigLoader.loadJsonFileFromJarPackage(DEFAULT_REDIS_CONFIG_FILE)
                : ConfigLoader.loadJsonFileFromLocalFileSystem(configFile, DEFAULT_REDIS_CONFIG_FILE);

            JSONObject configJson = new JSONObject(config);
            JSONArray redisSetupJson = configJson.getJSONArray(REDIS_SETUP_KEY);
            for(int i = 0; i < redisSetupJson.length(); ++i) {
                JSONObject masterSlavesJson = redisSetupJson.getJSONObject(i);
                String masterId = masterSlavesJson.getString(MASTER_ID_KEY);
                String masterHost = masterSlavesJson.getString(MASTER_HOST_KEY);
                _shardedRedisClients.put(masterId, new ShardedRedisClient(masterHost,
                        split(masterSlavesJson.getString(SLAVE_HOSTS_KEY), ',')));

                host2Ids.put(masterHost, masterId);
            }
        }catch(Exception e) {
            LOG.error(e.toString());

            throw e;
        }

        _hashing.initHosts(host2Ids);
    }

    public final String get(String key) {
        String shardId = _hashing.getHost(key);
        return _shardedRedisClients.get(shardId).get(key);
    }

    public final void set(String key, String value, int seconds) {
        String shardId = _hashing.getHost(key);
        _shardedRedisClients.get(shardId).set(key, value, seconds);
    }

    public final byte[] get(byte[] key) {
        String shardId = _hashing.getHost(key);
        return _shardedRedisClients.get(shardId).get(key);
    }

    public final void set(byte[] key, byte[] value, int seconds) {
        String shardId = _hashing.getHost(key);
        _shardedRedisClients.get(shardId).set(key, value, seconds);
    }

    public final List<String> multiGet(List<String> keys) {
        List<String> values = new ArrayList<String>();

        Map<String, List<String>> shardedKeys = new HashMap<String, List<String>>();
        for(String key: keys) {
            String shardId = _hashing.getHost(key);
            List<String> keyList = shardedKeys.get(shardId);
            if(keyList == null) {
                keyList = new ArrayList<String>();
                keyList.add(key);
                shardedKeys.put(shardId, keyList);
            } else
                keyList.add(key);
        }

        Map<String, String> keyValues = new HashMap<String, String>();
        for(Map.Entry<String, List<String>> entry: shardedKeys.entrySet()) {
            String shardId = entry.getKey();
            List<String> keyList = entry.getValue();
            List<String> valueList = _shardedRedisClients.get(shardId).multiGet(keyList);
            for(int i = 0; i < keyList.size(); ++i)
                keyValues.put(keyList.get(i), valueList.get(i));
        }

        for(String key: keys)
            values.add(keyValues.get(key));

        return values;
    }

    public final void multiSet(Map<String, String> keyValues, int seconds) {
        Map<String, Map<String, String>> shardedKeyValues = new HashMap<String, Map<String, String>>();
        for(Map.Entry<String, String> entry: keyValues.entrySet()) {
            String key = entry.getKey();
            String shardId = _hashing.getHost(key);
            Map<String, String> keyValueMap = shardedKeyValues.get(shardId);
            if(keyValueMap == null) {
                keyValueMap = new HashMap<String, String>();
                keyValueMap.put(key, entry.getValue());
                shardedKeyValues.put(shardId, keyValueMap);
            } else
                keyValueMap.put(key, entry.getValue());
        }

        for(Map.Entry<String, Map<String, String>> entry: shardedKeyValues.entrySet())
            _shardedRedisClients.get(entry.getKey()).multiSet(entry.getValue(), seconds);
    }

    public final void multiSet(List<String> keys, List<String> values, int seconds) {
        int keySize = keys.size();
        if(keySize != values.size()) {
            LOG.error("keys.size() != values.size()");
            return;
        }

        Map<String, Map<String, String>> shardedKeyValues = new HashMap<String, Map<String, String>>();
        for(int i = 0; i < keySize; ++i) {
            String key = keys.get(i);
            String shardId = _hashing.getHost(key);
            System.out.println("multiSet - shardId: " + shardId);
            Map<String, String> keyValueMap = shardedKeyValues.get(shardId);
            if(keyValueMap == null) {
                keyValueMap = new HashMap<String, String>();
                keyValueMap.put(key, values.get(i));
                shardedKeyValues.put(shardId, keyValueMap);
            } else
                keyValueMap.put(key, values.get(i));
        }

        for(Map.Entry<String, Map<String, String>> entry: shardedKeyValues.entrySet())
            _shardedRedisClients.get(entry.getKey()).multiSet(entry.getValue(), seconds);
    }

    private final List<String> split(String s, int delimiter) {
        List<String> splits = new ArrayList<String>();

        int start = 0;
        int end = s.indexOf(delimiter);
        while(end != -1) {
            splits.add(s.substring(start, end));

            start = end+1;
            end = s.indexOf(delimiter, start);
        }
        splits.add(s.substring(start));

        return splits;
    }

    public static void main(String[] args) throws Exception {
        Sharding sharding = new Sharding(new PlainConsistentHash(HashAlgorithm.PLAIN_HASH_ALGORITHM, 7));
        sharding.init();

        int count = 10000;
        List<String> keys = new ArrayList<String>();
        for(int i = 0; i < count; ++i)
            keys.add("test_key_" + i);
        List<String> setValues = new ArrayList<String>();
        for(int i = 0; i < count; ++i)
            setValues.add("test_value_" + i);
        long start = System.currentTimeMillis();
        sharding.multiSet(keys, setValues, 10);
        System.out.println("multiSet took " + (System.currentTimeMillis() - start) + " milliseconds.");
        /*
        List<String> getValues = sharding.multiGet(keys);
        for(String getValue: getValues)
            System.out.println("getValue: " + getValue);
        */
        System.out.println("Time took " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}