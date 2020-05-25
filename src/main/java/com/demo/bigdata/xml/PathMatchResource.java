/*
 * created on Dec 3, 2009
 */
package com.demo.bigdata.xml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * �ƥķ�ʽȡĳ·�µзƥ�ļ�
 * �Ant�·�ļ�дΪ�磺
 * "/WEB-INF/*Tag.xml""classpath:com/*-context.xml"
 * @author 
 */
public class PathMatchResource implements ResourcePath {

	private static final String CLASSPATH_URL_PREFIX = "classpath:";
	private String path;
	private Class<?> clazz;
	private ClassLoader classLoader;
	private Filter filter;
	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @return the clazz
	 */
	public Class<?> getClazz() {
		return clazz;
	}

	/**
	 * @param clazz the clazz to set
	 */
	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}

	/**
	 * @return the classLoader
	 */
	public ClassLoader getClassLoader() {
		return classLoader;
	}

	/**
	 * @param classLoader the classLoader to set
	 */
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Default constructor
	 */
	public PathMatchResource() {
		super();
	}

	/**
	 * @param path
	 * @param clazz
	 */
	public PathMatchResource(String path, Class<?> clazz) {
		super();
		this.path = path;
		this.clazz = clazz;
	}

	/**
	 * @param path
	 * @param classLoader
	 */
	public PathMatchResource(String path, ClassLoader classLoader) {
		super();
		this.path = path;
		this.classLoader = classLoader;
	}

	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.ResourcePath#getResources()
	 */
	public Resource[] getResources() throws IOException {
		String path = this.getPath();
		if(path==null){return null;}
		if(path.startsWith(CLASSPATH_URL_PREFIX)){
			path = path.substring(CLASSPATH_URL_PREFIX.length());
		}
		if(path.startsWith("/")){
			path = path.substring(1);
		}
		String baseDir = "";
		String pattern = "";
		int index = path.indexOf("*");
		if(index==-1){
			return this.getResources(path);
		}else{
			int index1 = path.substring(0, index).lastIndexOf("/");
			if(index1==-1){
				pattern = path;
			}else{
				baseDir = path.substring(0,index1);
				pattern = path.substring(index1+1);
			}
		}
		
		Resource[] resources = this.getResources(baseDir);
		
		if(resources==null){return null;}
		List<Resource> matchedResources = new ArrayList<Resource>();
		for(int i=0; i<resources.length; i++){
			String fileName = resources[i].getFileName();
			if(fileName!=null&&this.isMatch(fileName, pattern)){
				matchedResources.add(resources[i]);
			}
		}
		return (Resource[])matchedResources.toArray(new Resource[matchedResources.size()]);
	}
	
	/**
	 * ClassPathResourcegetResources()ȡĿ¼�µļ�Դ
	 * @param path 
	 * @return Resource[]
	 * @throws IOException
	 */
	protected Resource[] getResources(String path) throws IOException {
		ResourcePath resourcePath = null;
		if(this.getClazz()!=null){
			resourcePath = new ClassPathResource(path,this.getClazz());
		}else{
			resourcePath = new ClassPathResource(path,this.getClassLoader());
		}
		resourcePath.setFilter(this.getFilter());
		return resourcePath.getResources();
	}
	
	/**
	 * @param path ·
	 * @param pattern ƥģʽ
	 * @return ·ƥģʽ�Ƿ�ƥ
	 */
	protected boolean isMatch(String path, String pattern){
		int index = path.lastIndexOf("/");
		if(index!=-1){
			path = path.substring(index+1);
		}
		return (new AntPathMatcher()).match(pattern, path);
	}

	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.ResourcePath#getFilter()
	 */
	public Filter getFilter() {
		return this.filter;
	}

	/* (non-Javadoc)
	 * @see com.easydev.taglib.xml.ResourcePath#setFilter(com.easydev.taglib.xml.Filter)
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

}
