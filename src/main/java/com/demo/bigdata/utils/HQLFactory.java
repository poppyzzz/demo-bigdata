/*
 * created on 2016年3月18日
 */
package com.demo.bigdata.utils;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.demo.bigdata.xml.XmlBuildFactory;

/**
 * The factory of HiveQL which stored in the HiveQL.xml
 * @author Fangang
 */
public class HQLFactory extends XmlBuildFactory {
	private static final Map<String,Map<String,String>> hqlMap = new HashMap<String,Map<String,String>>();
	
	@Override
	protected void buildFactory(Element root) {
		NodeList nodeList = root.getChildNodes();
		for (int i = 0; i <= nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if (!(node instanceof Element)) {
				continue;
			}
			Element element = (Element) node;
			loadBean(element);
		}
	}
	
	/**
	 * load a hql into memory
	 * @param element
	 */
	protected void loadBean(Element element) {
		if ("hql".equalsIgnoreCase(element.getNodeName())) {
			String schema = element.getAttribute("schema");
			if (schema == null) {
				return;
			}
			String tableName = element.getAttribute("tableName");
			if (tableName == null) {
				return;
			}
			String hql = this.getNodeValue(element);
			if (hql == null) {
				return;
			}
			
			
			Map<String,String> table = hqlMap.get(schema);
			if(table==null) table = new HashMap<String,String>();
			table.put(tableName, hql);
			hqlMap.put(schema, table);
		}
	}
	
	/**
	 * get value of a node like this: <node>value</node>
	 * @param element
	 * @return value of the node
	 */
	protected String getNodeValue(Element element) {
		if (element != null && element.getFirstChild() != null) {
			StringBuffer sb = new StringBuffer();
			NodeList children = element.getChildNodes();
			int count = children.getLength();
			for (int i = 0; i < count; i++) {
				Node child = children.item(i);
				sb.append(child.getNodeValue());
			}
			return sb.toString().trim();
		}
		return null;
	}
	
	/**
	 * get HQL by id
	 * @param id
	 * @return the HQL
	 */
	public static String getHQL(String schema, String tableName) {
		if(hqlMap.isEmpty()) {
			HQLFactory factory = new HQLFactory();
			factory.initFactory("HiveQL.xml");
		}
		
		String hql = hqlMap.get(schema).get(tableName);
		if(hql==null) throw new RuntimeException("No such hql:[schema:"+schema+",tableName:"+tableName+"]");
		return hql;
	}
	
}
