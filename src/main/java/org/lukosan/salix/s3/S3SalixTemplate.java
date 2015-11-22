package org.lukosan.salix.s3;

import org.lukosan.salix.SalixTemplate;

public class S3SalixTemplate implements SalixTemplate {
	
	private static final long serialVersionUID = 1L;
	
	private String scope;
	private String name;
	private String source;

	public S3SalixTemplate() {
		super();
	}
	
	public S3SalixTemplate(String scope, String name, String source) {
		this();
		setScope(scope);
		setName(name);
		setSource(source);
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
}