package org.lukosan.salix.s3;

import java.util.HashMap;
import java.util.Map;

import org.lukosan.salix.SalixConfiguration;

public class S3SalixConfiguration implements SalixConfiguration {

	private static final long serialVersionUID = 1L;
	
	String scope;
	String target;
	Map<String, Object> map = new HashMap<String, Object>();

	public S3SalixConfiguration() {
		super();
	}
	
	public S3SalixConfiguration(String scope, String target, Map<String, Object> map) {
		this();
		setScope(scope);
		setTarget(target);
		setMap(map);
	}

	public String getScope() {
		return scope;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public Map<String, Object> getMap() {
		return map;
	}
	public void setMap(Map<String, Object> map) {
		this.map = map;
	}
	
}
