package com.xc.storm.esper.bolt;

import java.util.Map;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.xc.storm.esper.VisitorBean;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/** 
* @ClassName: CountBolt 
* @Description: 统计节点，并输出到控制台
* @author xuechen
* @date 2017年1月13日 下午5:36:46
*  
*/
public class CountBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private EPServiceProvider ePService;

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.initEpser();
	}
	
	//初始化Epser
	private void initEpser() {
		Configuration configuration = new Configuration();
		configuration.addEventType("Vistor", VisitorBean.class.getName());
		
		ePService = EPServiceProviderManager.getDefaultProvider(configuration);
		ePService.initialize();
		
		//统计60s内每种浏览器的访客数，每10s生成一个快照
		String epl = "select count(Vistor.ip) as num, Vistor.browse as browse from Vistor.win:time(60 seconds) group by Vistor.browse output snapshot every 2 sec";
		EPStatement stat = ePService.getEPAdministrator().createEPL(epl);
		
		stat.addListener(new UpdateListener() {
			
			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if(newEvents != null) {
					for(EventBean event : newEvents) {
						Long num = (Long) event.get("num");
						String browse = (String) event.get("browse");
						System.out.println("---------------" + browse + ":" + num + "-----------------");
					}
				}
				
			}
		});
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		VisitorBean vistor = (VisitorBean) input.getValue(0);
		ePService.getEPRuntime().sendEvent(vistor);

	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
