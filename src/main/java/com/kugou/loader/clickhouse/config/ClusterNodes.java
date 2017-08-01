package com.kugou.loader.clickhouse.config;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.UUID;

/**
 * Created by jaykelin on 2017/7/18.
 */
public class ClusterNodes {

    private String cluster;
    private Integer shard_num;
    private Integer shard_weight;
    private JSONArray hosts;
    private final String key;


    public ClusterNodes(String hostaddress){
        this.cluster = hostaddress.trim();
        this.shard_num = 1;
        this.shard_weight = 1;
        this.hosts = new JSONArray();
        hosts.put(hostaddress.trim());
        this.key = this.cluster+"_shard_num_"+this.shard_num;
    }

    public ClusterNodes(String cluster, int shard_num, Integer shard_weight,
                        String host_address_array_json) throws JSONException {
        this.cluster = cluster;
        this.shard_num = shard_num;
        this.shard_weight = shard_weight;
        this.hosts = new JSONArray(host_address_array_json);
        this.key = this.cluster+"_shard_num_"+this.shard_num;
    }

    public String getKey(){
//       return  new StringBuilder(cluster).append(shard_num).hashCode();
        return this.key;
    }

    public String getCluster(){
        return this.cluster;
    }

    public int getShardNum(){
        return this.shard_num;
    }

    public int getShardWeight(){
        return shard_weight;
    }

    public int getHostsCount(){
        return hosts == null ? 0 : hosts.length();
    }

    public String hostAddress(int index) throws JSONException {
        return hosts == null ? null : hosts.getString(index);
    }

    public String toString(){
        JSONObject obj =null;
        try {
            obj = new JSONObject();
            obj.put("cluster", this.cluster);
            obj.put("shard_num", this.shard_num);
            obj.put("nodes", this.hosts);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        if (null != obj){
            return obj.toString();
        }
        return "";
    }
}
