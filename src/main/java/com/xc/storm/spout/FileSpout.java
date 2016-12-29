package com.xc.storm.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** 
* @ClassName: FileSpout 
* @Description: 读取文件，当作数据源
* @author xuechen
* @date 2016年12月29日 下午2:47:35 
*  
*/
public class FileSpout implements IRichSpout {
	
	private SpoutOutputCollector collector;
	FileInputStream fis;
	InputStreamReader is;
	BufferedReader br;

	/**
	 * 打开文件流
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
		String file = "demo.txt";
		
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
				this.collector.emit(new Values(str));
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
		declarer.declare(new Fields("str"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
