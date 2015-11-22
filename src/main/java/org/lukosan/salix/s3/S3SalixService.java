package org.lukosan.salix.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lukosan.salix.SalixConfiguration;
import org.lukosan.salix.SalixResource;
import org.lukosan.salix.SalixService;
import org.lukosan.salix.SalixTemplate;
import org.lukosan.salix.SalixUrl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.mrbean.MrBeanModule;

public class S3SalixService implements SalixService {

	private static final Log logger = LogFactory.getLog(S3SalixService.class);
	
	@Value("${salix.s3.bucket}")
	private String bucketName;
	
	private AmazonS3 client;
	
	private ObjectMapper mapper;
	
	private static final String delimiter = "/";

	public S3SalixService() {
		client = new AmazonS3Client();
		mapper = new ObjectMapper();
		mapper.registerModule(new MrBeanModule());
	}
	
	private List<String> listDirectoriesInDirectory(String prefix) {
		ObjectListing objects = listObjectsInDirectory(prefix);
	    return objects.getCommonPrefixes().stream().map(s -> s.substring(0, s.length() - 1)).collect(Collectors.toList());
	}
	
	private List<String> listKeysInDirectory(String prefix) {
	    ObjectListing objects = listObjectsInDirectory(prefix);
	    return objects.getObjectSummaries().stream().map(s -> s.getKey()).collect(Collectors.toList());
	}
	
	private ObjectListing listObjectsInDirectory(String prefix) {
	    if (StringUtils.hasText(prefix) && !prefix.endsWith(delimiter)) {
	        prefix += delimiter;
	    }
	    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
	            .withBucketName(bucketName).withPrefix(prefix)
	            .withDelimiter(delimiter);
	    try {
	    	return client.listObjects(listObjectsRequest);
	    } catch(AmazonServiceException ase) {
	    	logger.error(ase);
	    	throw new RuntimeException(ase);
	    }
	}
	
	private S3Object getObject(String key) {
		try {
			return client.getObject(new GetObjectRequest(bucketName, key));
		} catch(AmazonServiceException ase) {
			logger.error(ase);
	    	throw new RuntimeException(ase);
	    }
	}
	
	@Override
	public Set<String> scopes() {
		return new HashSet<String>(listDirectoriesInDirectory(""));
	}

	@Override
	public List<SalixConfiguration> configurationsIn(String scope) {
		List<SalixConfiguration> configs = new ArrayList<SalixConfiguration>();
		for(String key : listKeysInDirectory(scope + delimiter + "configurations")) {
			configs.add(configuration(scope, key));
		}
		return configs.stream().filter(c -> null != c).collect(Collectors.toList());
	}

	@Override
	public List<SalixConfiguration> configurationsFor(String target) {
		List<SalixConfiguration> configs = new ArrayList<SalixConfiguration>();
		for(String scope : scopes()) {
			for(String key : listKeysInDirectory(scope + delimiter + "configurations")) {
				if(key.equalsIgnoreCase(target))
					configs.add(configuration(scope, key));
			}
		}
		return configs.stream().filter(c -> null != c).collect(Collectors.toList());
	}

	@Override
	public SalixConfiguration configuration(String scope, String target) {
		if(StringUtils.isEmpty(target))
			return null;
		try {
			S3Object object = getObject(scope + delimiter + "configurations" + delimiter + target);
			return mapper.readValue(object.getObjectContent(), S3SalixConfiguration.class);
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public SalixConfiguration save(String scope, String target, Map<String, Object> map) {
		try {
			PipedOutputStream out = new PipedOutputStream();
			PipedInputStream in = new PipedInputStream(out);
			S3SalixConfiguration configuration = new S3SalixConfiguration(scope, target, map);
			mapper.writeValue(out, configuration);
			PutObjectRequest req = new PutObjectRequest(bucketName, scope + delimiter + "configurations" + delimiter + target, in, new ObjectMetadata());
			client.putObject(req);
			return configuration;
		} catch (IOException e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public List<SalixUrl> activeUrls() {
		// all are active
		return allUrls();
	}

	@Override
	public List<SalixUrl> urlsIn(String scope) {
		List<SalixUrl> urls = new ArrayList<SalixUrl>();
		String path = scope + delimiter + "urls";
		for(String key : listKeysInDirectory(path)) {
			if(! key.endsWith(delimiter))
				urls.add(url(key.substring(path.length()+1).replace('_', '/'), scope));
		}
		return urls.stream().filter(c -> null != c).collect(Collectors.toList());
	}

	@Override
	public SalixUrl url(String url, String scope) {
		if(StringUtils.isEmpty(url))
			return null;
		try {
			S3Object object = getObject(scope + delimiter + "urls" + delimiter + url.replace('/', '_'));
			return mapper.readValue(object.getObjectContent(), S3SalixUrl.class);
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public SalixUrl save(SalixUrl salixUrl) {
		try {
			PipedOutputStream out = new PipedOutputStream();
			PipedInputStream in = new PipedInputStream(out);
			mapper.writeValue(out, salixUrl);
			PutObjectRequest req = new PutObjectRequest(bucketName, salixUrl.getScope() + delimiter + "urls" + delimiter + salixUrl.getUrl().replace('/',  '_'), in, new ObjectMetadata());
			client.putObject(req);
			return salixUrl;
		} catch (IOException e) {
			logger.error(e);
			return salixUrl;
		}
	}

	@Override
	public SalixUrl save(String scope, String url, int status, String view, LocalDateTime published, LocalDateTime removed, Map<String, Object> map) {
		S3SalixUrl s3url = new S3SalixUrl(scope, url, view, map);
		return save(s3url);
	}

	@Override
	public SalixTemplate template(String name) {
		if(StringUtils.isEmpty(name))
			return null;
		try {
			S3Object object = getObject("templates" + delimiter + name);
			return mapper.readValue(object.getObjectContent(), S3SalixTemplate.class);
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public SalixTemplate template(String name, String scope) {
		if(StringUtils.isEmpty(name))
			return null;
		try {
			S3Object object = getObject(scope + delimiter + "templates" + delimiter + name);
			return new S3SalixTemplate(scope, name, IOUtils.toString(object.getObjectContent()));
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public SalixTemplate save(String scope, String name, String source) {
		try {
			S3SalixTemplate template = new S3SalixTemplate(scope, name, source);
			InputStream in = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8));
			PutObjectRequest req = new PutObjectRequest(bucketName, scope + delimiter + "templates" + delimiter + name, in, new ObjectMetadata());
			client.putObject(req);
			return template;
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public SalixResource resource(String sourceId, String scope) {
		return new S3SalixResource(scope, sourceId);
	}

	@Override
	public SalixResource save(String scope, String sourceId, String sourceUri, Map<String, Object> map) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SalixResource save(String scope, String sourceId, String sourceUri, String contentType, String text) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SalixResource save(String scope, String sourceId, String sourceUri, String contentType, byte[] bytes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SalixTemplate> templatesIn(String scope) {
		List<SalixTemplate> templates = new ArrayList<SalixTemplate>();
		String path = scope + delimiter + "templates";
		for(String key : listKeysInDirectory(path)) {
			templates.add(template(key.substring(path.length()+1), scope));
		}
		return templates.stream().filter(c -> null != c).collect(Collectors.toList());
	}

	@Override
	public List<SalixResource> resourcesIn(String scope) {
		List<SalixResource> resources = new ArrayList<SalixResource>();
		String path = scope + delimiter + "resources";
		for(String key : listKeysInDirectory(path)) {
			resources.add(resource(key.substring(path.length()+1), scope));
		}
		return resources.stream().filter(c -> null != c).collect(Collectors.toList());
	}
}
