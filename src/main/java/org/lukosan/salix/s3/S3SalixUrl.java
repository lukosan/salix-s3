package org.lukosan.salix.s3;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.lukosan.salix.SalixUrl;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class S3SalixUrl implements SalixUrl {

	private static final long serialVersionUID = 1L;
	
	private String scope;
	private String url;
	private String view;
	private Map<String, Object> map = new HashMap<String, Object>();
	
	public S3SalixUrl() {
		super();
	}
	public S3SalixUrl(String scope, String url, String view, Map<String, Object> map) {
		this();
		setScope(scope);
		setUrl(url);
		setView(view);
		setMap(map);
	}
	
	public String getScope() {
		return scope;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getView() {
		return view;
	}
	public void setView(String view) {
		this.view = view;
	}
	public Map<String, Object> getMap() {
		return map;
	}
	public void setMap(Map<String, Object> map) {
		this.map = map;
	}
	@JsonIgnore
	public LocalDateTime getPublished() {
		return LocalDateTime.now().minusYears(1L);
	}
	@JsonIgnore
	public LocalDateTime getRemoved() {
		return null;
	}
	@JsonIgnore
	public int getStatus() {
		return 200;
	}
}
