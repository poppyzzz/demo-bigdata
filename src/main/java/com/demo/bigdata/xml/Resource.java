/*
 * created on Nov 30, 2009
 */
package com.demo.bigdata.xml;

import java.io.IOException;
import java.io.InputStream;


/**
 * ȡһ�ļ�Դ�Ĵ�ӿ�
 * @author 
 */
public interface Resource {
	/**
	 * һ�ļж�ȡһInputStream
	 * @return InputStream
	 * @throws IOException 
	 */
	public InputStream getInputStream()throws IOException;
	
	/**
	 * @return �ļ�Դ�Ϣ�ڵԺ͸�
	 */
	public String getDescription();

	/**
	 * @param filter �ļ�
	 */
	public void setFilter(Filter filter);
	
	/**
	 * @return �ļ�
	 */
	public Filter getFilter();
	
	/**
	 * @return �ļ�Դ�ļ·
	 */
	public String getFileName();
}
