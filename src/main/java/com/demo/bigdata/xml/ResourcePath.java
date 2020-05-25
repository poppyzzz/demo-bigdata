/*
 * created on Nov 30, 2009
 */
package com.demo.bigdata.xml;

import java.io.IOException;

/**
 * ȡһ·�ļ�Դ�Ĵ�
 * @author 
 */
public interface ResourcePath {
	
	/**һ·�ж�ȡ�ļ�װװ�з�
	 * @return Array of Resource װ
	 * @throws IOException
	 */
	public Resource[] getResources() throws IOException;

	/**
	 * @param filter �ļ�
	 */
	public void setFilter(Filter filter);
	
	/**
	 * @return �ļ�
	 */
	public Filter getFilter();
}
