package com.xc.storm.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/** 
* @ClassName: PrintBolt 
* @Description: TODO
* @author xuechen
* @date 2016年12月29日 下午4:18:28 
*  
*/
public class PrintBolt extends BaseRichBolt {
	
	private HashMap<String, Long> countMap = null;

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.countMap = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.countMap.put(word, count);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		System.out.println("---------final count-----------");
		Set<String> sets = this.countMap.keySet();
		Iterator<String> it = sets.iterator();
		while(it.hasNext()) {
			String word = it.next();
			Long count = this.countMap.get(word);
			System.out.println(word + " : " + count);
		}
		System.out.println("----------count over------------");
	}
	
	

}
