/*
 * created on 2009-11-16 
 */
package com.demo.bigdata.xml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * DOM�ķ�ʽȡ�ͽ�XML�ļģ�͡�
 * ͨ�initFactory()ָһXML�ļж�ȡݣ�
 * Ȼ�󽫶�ȡ�װ�ص�һ�С�
 * <p>м̳ͨ�buildFactory()ʵ�֣�
 * �嶨ν�ȡ�װ�ص�һ�С�
 * @author FanGang
 */
public abstract class XmlBuildFactory {
	private static final Log log = LogFactory.getLog(XmlBuildFactory.class);
	private boolean validating = false;
	private boolean namespaceAware = false;
	private Filter filter = null;
	private String[] paths = null;
	
	/**
	 * ȷ�ڽ�XMLʱ�Ƿṩ XML ƿռ�֧�ֵĽ�
	 * @return the namespaceAware
	 */
	public boolean isNamespaceAware() {
		return namespaceAware;
	}

	/**
	 * ָ�ɴ˴�ɵĽṩ XML ƿռ֧
	 * @param namespaceAware the namespaceAware to set
	 */
	public void setNamespaceAware(boolean namespaceAware) {
		this.namespaceAware = namespaceAware;
	}

	/**
	 * ȷ�ڽ�XMLʱ�Ƿ�ڽ�ʱ֤ XML �ݡ�
	 * @return the validating
	 */
	public boolean isValidating() {
		return validating;
	}

	/**
	 * ָ�ɴ˴�ɵĽ�֤ XML �ĵ�
	 * @param validating the validating to set
	 */
	public void setValidating(boolean validating) {
		this.validating = validating;
	}

	/**
	 * ȡһ�ļ�ЩӦ�Ա˳�
	 * Ĭ�ϵĹǽ�*.xml*.XML�ļ˳�
	 * @return the filter �ļ�
	 */
	public Filter getFilter() {
		if(filter==null){
			filter = new Filter(){

				public boolean isSatisfied(String fileName) {
					if(fileName.endsWith(".xml")||fileName.endsWith(".XML")){return true;}
					else {return false;}
				}};
		}
		return filter;
	}

	/**
	 * �ṩһ�ļļ�
	 * @param filter the filter to set
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	/**
	 * ʼ�·ȡXML�ļ�XML�ļеװ�ص�
	 * @param path XML·
	 */
	public void initFactory(String path){
		if(this.paths==null) this.paths = new String[]{path};
		if(findOnlyOneFileByClassPath(path)){return;}
		if(findResourcesByUrl(path)){return;}
		if(findResourcesByFile(path)){return;}
		throw new RuntimeException("Cannot open files in the path:"+path);
	}
	
	/**
	 * ʼ�·�бζ�ȡXML�ļ�XML�ļеװ�ص�
	 * @param paths ·�б�
	 */
	public void initFactory(String[] paths){
		this.paths = paths;
		for(int i=0; i<paths.length; i++){
			initFactory(paths[i]);
		}
	}
	
	/**
	 * �³�ʼʼĲ�Ϊһ�γ�ʼ�õĲ�
	 */
	public void reloadFactory(){
		initFactory(this.paths);
	}
	
	/**
	 * ClassLoader�ķ�ʽͼһ�ļ�<code>readXmlStream()</code>�н�
	 * @param path XML�ļ�·
	 * @return �Ƿ�ɹ�
	 */
	protected boolean findOnlyOneFileByClassPath(String path){
		boolean success = false;
		try {
			Resource resource = new ClassPathResource(path, this.getClass());
			resource.setFilter(this.getFilter());
			InputStream is = resource.getInputStream();
			if(is==null){return false;}
			readXmlStream(is);
			success = true;
		} catch (SAXException e) {
			log.debug("Error when findOnlyOneFileByClassPath:"+path,e);
		} catch (IOException e) {
			log.debug("Error when findOnlyOneFileByClassPath:"+path,e);
		} catch (ParserConfigurationException e) {
			log.debug("Error when findOnlyOneFileByClassPath:"+path,e);
		}
		return success;
	}
	
	/**
	 * URL�ķ�ʽͼһĿ¼�е�XML�ļ�<code>readXmlStream()</code>�н�
	 * @param path XML�ļ�·
	 * @return �Ƿ�ɹ�
	 */
	protected boolean findResourcesByUrl(String path){
		boolean success = false;
		try {
			ResourcePath resourcePath = new PathMatchResource(path, this.getClass());
			resourcePath.setFilter(this.getFilter());
			Resource[] loaders = resourcePath.getResources();
			for(int i=0; i<loaders.length; i++){
				InputStream is = loaders[i].getInputStream();
				if(is!=null){
					readXmlStream(is);
					success = true;
				}
			}
		} catch (SAXException e) {
			log.debug("Error when findResourcesByUrl:"+path,e);
		} catch (IOException e) {
			log.debug("Error when findResourcesByUrl:"+path,e);
		} catch (ParserConfigurationException e) {
			log.debug("Error when findResourcesByUrl:"+path,e);
		}
		return success;
	}
	
	/**
	 * File�ķ�ʽͼ�ļ�<code>readXmlStream()</code>
	 * @param path XML�ļ�·
	 * @return �Ƿ�ɹ�
	 */
	protected boolean findResourcesByFile(String path){
		boolean success = false;
		FileResource loader = new FileResource(new File(path));
		loader.setFilter(this.getFilter());
		try {
			Resource[] loaders = loader.getResources();
			if(loaders==null){return false;}
			for(int i=0; i<loaders.length; i++){
				InputStream is = loaders[i].getInputStream();
				if(is!=null){
					readXmlStream(is);
					success = true;
				}
			}
		} catch (IOException e) {
			log.debug("Error when findResourcesByFile:"+path,e);
		} catch (SAXException e) {
			log.debug("Error when findResourcesByFile:"+path,e);
		} catch (ParserConfigurationException e) {
			log.debug("Error when findResourcesByFile:"+path,e);
		}
		return success;
	}

	/**
	 * ȡһXML�ļ�ElementʽȡXML�ĸ�
	 * Ȼ�<code>buildFactory(Element)</code>
	 * @param inputStream �ļ�
	 * @throws SAXException �׳�SAXXML�ļ�гֵ쳣
	 * @throws IOException �׳ļ�дгֵ쳣
	 * @throws ParserConfigurationException �׳�ճXML�гֵ쳣
	 */
	protected void readXmlStream(InputStream inputStream) throws SAXException, IOException, ParserConfigurationException{
		if(inputStream==null){
			throw new ParserConfigurationException("Can't parse source because of InputStream is null!");
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(this.isValidating());
        factory.setNamespaceAware(this.isNamespaceAware());
        DocumentBuilder build = factory.newDocumentBuilder();
        Document doc = build.parse(new InputSource(inputStream));
        Element root = doc.getDocumentElement();
        buildFactory(root);
	}
	
	/**
	 * �ô�һXML�ļж�ȡݹ�
	 * @param root һXML�ļж�ȡݵĸ�
	 */
	protected abstract void buildFactory(Element root);
	
}
