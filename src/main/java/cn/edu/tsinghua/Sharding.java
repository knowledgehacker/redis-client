package cn.edu.tsinghua;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.util.ConfigLoader;

public class Sharding {
	private static final Logger LOG = LoggerFactory.getLogger(Sharding.class);

	private static final String DEFAULT_REDIS_CONFIG_FILE = "/redis.json";

	private static final String REDIS_SETUP_KEY = "redis-setup";
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

    private static void main(String[] args) throws Exception {
        Sharding sharding = new Sharding(new PlainConsistentHash(HashAlgorithm.PLAIN_HASH_ALGORITHM, 100));
        sharding.init();

        // TODO: do some test
    }
}