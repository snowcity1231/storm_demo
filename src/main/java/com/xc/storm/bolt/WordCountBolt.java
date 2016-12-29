package com.xc.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
* @ClassName: WordCountBolt 
* @Description: 统计单词Bolt
* @author xuechen
* @date 2016年12月29日 下午4:10:17 
*  
*/
public class WordCountBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private HashMap<String, Long> countMap = null;

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.countMap = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
//		String word = input.getString(0);
		Long count = 0L;
		if(countMap.containsKey(word)) {
			count = countMap.get(word);
		}
		count++;
		this.countMap.put(word, count);
		this.collector.emit(new Values(word, count));

	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));

	}

}
