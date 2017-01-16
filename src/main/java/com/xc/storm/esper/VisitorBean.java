package com.xc.storm.esper;

/** 
* @ClassName: VisitorBean 
* @Description: TODO
* @author xuechen
* @date 2017年1月13日 下午5:49:45
*  
*/
public class VisitorBean {

	private String ip;
	private String browse;
	
	public static VisitorBean parse(String info) {
		VisitorBean visitorBean = new VisitorBean();
		String[] infos = info.split("\\|");
		String[] ips = infos[0].split(":");
		String[] browses = infos[1].split(":");
		visitorBean.setIp(ips[1]);
		visitorBean.setBrowse(browses[1]);
		return visitorBean;
	}
	/**
	 * @return the ip
	 */
	public String getIp() {
		return ip;
	}
	/**
	 * @param ip the ip to set
	 */
	public void setIp(String ip) {
		this.ip = ip;
	}
	/**
	 * @return the browse
	 */
	public String getBrowse() {
		return browse;
	}
	/**
	 * @param browse the browse to set
	 */
	public void setBrowse(String browse) {
		this.browse = browse;
	}
	
	
}
