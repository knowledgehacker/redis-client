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

import util.ConfigLoader;

public class PreSharding {
	private static final Logger LOG = LoggerFactory.getLogger(PreSharding.class);

	private static final String DEFAULT_REDIS_CONFIG_FILE = "/redis.json";

	private static final String REDIS_SETUP_KEY = "redis-setup";
	private static final String MASTER_ID_KEY = "master.id";
	private static final String MASTER_HOST_KEY = "master.host";
	private static final String SLAVE_HOSTS_KEY = "slave.hosts";

	private final Map<Integer, MasterSlaves> _masterSlaves = new HashMap<Integer, MasterSlaves>();

	public PreSharding() {
		this(null);
	}

	public PreSharding(String configFile) {
		loadConfig(configFile);
	}

	private void loadConfig(String configFile) {
        	String config = (configFile == null) ? ConfigLoader.loadJsonFileFromJarPackage(DEFAULT_REDIS_CONFIG_FILE)
            		: ConfigLoader.loadJsonFileFromLocalFileSystem(configFile);

		try {
			JSONObject configJson = new JSONObject(config);
			JSONArray redisSetupJson = configJson.getJSONArray("redis.setup");
			for(int i = 0; i < redisSetupJson.length(); ++i) {
				JSONObject masterSlavesJson = redisSetupJson.getJSONObject(i);
				int masterId = masterSlavesJson.getInt(MASTER_ID_KEY);
				MasterSlaves masterSlaves = new MasterSlaves(masterSlavesJson.getString(MASTER_HOST_KEY), masterSlavesJson.getString(SLAVE_HOSTS_KEY));
				_masterSlaves.put(masterId, masterSlaves);
			}
		}catch(JSONException jsone) {
                        LOG.error(jsone.toString());
                        return;
                }
	}

	private class MasterSlaves {
		private final String _master;
		private final List<String> _slaves;
	
		public MasterSlaves(String master, String slaves) {
			_master = master;
			_slaves = split(slaves, ',');	
		}

		private final List<String> split(String s, int delimeter) {
			List<String> splits = new ArrayList<String>();

			int start = 0;
			int end = s.indexOf(delimeter);
			while(end != -1) {
				splits.add(s.substring(start, end));

				start = end+1;
				end = s.indexOf(delimeter, start);
			}
			splits.add(s.substring(start));

			return splits;
		}

		public final String getMaster() {
			return _master;
		}

		public final List<String> getSlaves() {
			return _slaves;
		}
	}
}
