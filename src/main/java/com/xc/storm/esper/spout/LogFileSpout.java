package com.xc.storm.esper.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import com.xc.storm.esper.VisitorBean;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** 
* @ClassName: LogFileSpout 
* @Description: 读取日志文件作为消息源
* @author xuechen
* @date 2017年1月13日 下午5:38:56
*  
*/
public class LogFileSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	FileInputStream fis;
	InputStreamReader is;
	BufferedReader br;

	/**
	 * 打开文件流
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
		String file = "website.log";
		
		try {
			this.fis = new FileInputStream(file);
			this.is = new InputStreamReader(fis);
			this.br = new BufferedReader(is);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void close() {

	}

	public void activate() {

	}

	public void deactivate() {

	}

	public void nextTuple() {
		String str = "";
		
		try {
			while((str = this.br.readLine()) != null) {
				this.collector.emit(new Values(VisitorBean.parse(str)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("vistor"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
