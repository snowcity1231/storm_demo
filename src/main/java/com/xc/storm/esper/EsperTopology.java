package com.xc.storm.esper;

import com.xc.storm.esper.bolt.CountBolt;
import com.xc.storm.esper.spout.LogFileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/** 
* @ClassName: EsperTopology 
* @Description: strom整合Epser
* @author xuechen
* @date 2017年1月13日 下午5:22:34
*  
*/
public class EsperTopology {

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("FileSpout", new LogFileSpout(), 1);
		builder.setBolt("CountBolt", new CountBolt(), 1).shuffleGrouping("FileSpout");
		
		Config config = new Config();
		config.setDebug(false);
		config.setMaxTaskParallelism(1);
		
		//本地模拟一个完整的storm集群
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCountTopology", config, builder.createTopology());
	}
}
