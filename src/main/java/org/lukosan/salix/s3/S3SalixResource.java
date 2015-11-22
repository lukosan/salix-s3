package org.lukosan.salix.s3;

import org.lukosan.salix.SalixResource;
import org.lukosan.salix.SalixResourceType;

public class S3SalixResource implements SalixResource {

	private static final long serialVersionUID = 1L;

	private String scope;
	private String sourceId;

	public S3SalixResource() {
		super();
	}
	
	public S3SalixResource(String scope, String sourceId) {
		this();
		this.scope = scope;
		this.sourceId = sourceId;
	}
	
	public String getScope() {
		return scope;
	}

	public String getSourceId() {
		return sourceId;
	}

	public String getSourceUri() {
		return scope + "/resources/" + sourceId;
	}

	public String getResourceId() {
		return getSourceId();
	}

	public String getResourceUri() {
		return "/salix/resource/s3/" + getResourceId();
	}

	private static final String[] txts = { "txt", "css" };
	private static final String[] jsons = { "json", "yml" };
	
	public SalixResourceType getResourceType() {
		for(String suffix : txts)
			if(getResourceId().endsWith("." + suffix))
				return SalixResourceType.TEXT;
		for(String suffix : jsons)
			if(getResourceId().endsWith("." + suffix))
				return SalixResourceType.JSON;
		return SalixResourceType.BINARY;
	}

	public String getContentType() {
		return null;
	}	
}