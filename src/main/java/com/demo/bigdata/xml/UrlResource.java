/*
 * created on Nov 30, 2009
 */
package com.demo.bigdata.xml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * URLȡ�ļ�Դ�Ĵ�
 * @author 
 */
public class UrlResource implements Resource, ResourcePath {

	private static final Log log = LogFactory.getLog(UrlResource.class);
	private URL url;
	private Filter filter = null;
	
	/**
	 * @return the url
	 */
	public URL getUrl() {
		return url;
	}
	
	/**
	 * Constructor for url
	 * @param url
	 * @throws FileNotFoundException 
	 */
	public UrlResource(URL url) throws FileNotFoundException {
		super();
		this.url = url;
		if(url==null){
			throw new FileNotFoundException("No url to be found!");
		}
		log.debug("loading "+this.getDescription());
	}
	
	/**
	 * <code>url.openStream()</code>ȡһ�ļ�
	 * @return InputStream
	 * @exception IOException
	 */
	public InputStream getInputStream() throws IOException {
		URL url = this.getUrl();
		if(url==null){
			throw new FileNotFoundException("No url to be found!");
		}
		Filter filter = this.getFilter();
		if(filter==null||!filter.isSatisfied(url.getFile())){
			return null;
		}
		return url.openStream();
	}
	
	/**
	 * URLȡĳĿ¼�µļ�
	 * URLһjar�JarResourceȡ�ļ�
	 * URLһfile�FileResourceȡ�ļ�
	 * @return Resource[]
	 * @exception IOException
	 */
	public Resource[] getResources() throws IOException {
		URL url = this.getUrl();
		if(url==null){
			throw new FileNotFoundException("No url to be found!");
		}
		if(this.isJarResource(url)){
			JarResource resource = new JarResource(url.openConnection());
			resource.setFilter(this.getFilter());
			return resource.getResources();
		}else{
			File file = new File(URLDecoder.decode(url.getFile(), "UTF-8"));
			FileResource resource = new FileResource(file);
			resource.setFilter(this.getFilter());
			return resource.getResources();
		}
	}
	
	private boolean isJarResource(URL url){
		String protecol = url.getProtocol();
		if("jar".equals(protecol)||"zip".equals(protecol)||"wsjar".equals(protecol)){
			return true;
		}else{
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.Resource#getDescription()
	 */
	public String getDescription() {
		if(this.getUrl()==null){
			return "UrlResource: [url:null]";
		}
		return (new StringBuffer("UrlResource:[file:"))
					.append(this.getUrl().getFile()).append(",protocol:")
					.append(this.getUrl().getProtocol())
					.append("]").toString();
	}
	
	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.Resource#getFilter()
	 */
	public Filter getFilter() {
		return this.filter;
	}
	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.Resource#setFilter(com.easydev.taglib.xml.Filter)
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.Resource#getFileName()
	 */
	public String getFileName() {
		if(this.getUrl()==null){return null;}
		return this.getUrl().getFile();
	}
}
