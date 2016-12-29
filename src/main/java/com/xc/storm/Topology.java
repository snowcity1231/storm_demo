package com.xc.storm;

import com.xc.storm.bolt.PrintBolt;
import com.xc.storm.bolt.SplitSentenceBolt;
import com.xc.storm.bolt.WordCountBolt;
import com.xc.storm.spout.FileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/** 
* @ClassName: Topology 
* @Description: 单词计数拓扑
* @author xuechen
* @date 2016年12月29日 下午2:46:29 
*  
*/
public class Topology {
	
	private static final String FILE_SPOUT_ID	= "file-spout"; 
	private static final String SPLIT_BOLT_ID	= "split-bolt";
	private static final String COUNT_BOLT_ID	= "count-bolt";
	private static final String PRINT_BOLT_ID	= "print-bolt";

	public static void main(String[] args) throws InterruptedException {
		//拓扑流程
		TopologyBuilder builder = new TopologyBuilder();
		 
		// 设置数据读取节点以及并发数
		builder.setSpout(FILE_SPOUT_ID, new FileSpout(), 1);
		
		//设置分割句子节点
		builder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt(), 3).shuffleGrouping(FILE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), 3).shuffleGrouping(SPLIT_BOLT_ID);
		builder.setBolt(PRINT_BOLT_ID, new PrintBolt(), 3).shuffleGrouping(COUNT_BOLT_ID);
		
		Config config = new Config();
		config.setDebug(false);
		config.setMaxTaskParallelism(1);
		
		//本地模拟一个完整的storm集群
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCountTopology", config, builder.createTopology());
		
		Thread.sleep(5000);
		cluster.shutdown();
	}
}
