package org.lukosan.salix.s3;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lukosan.salix.fs.FsClient;
import org.lukosan.salix.fs.FsSalixService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3FsClient implements FsClient {

	private static final Log logger = LogFactory.getLog(S3FsClient.class);
	
	@Value("${salix.s3.bucket}")
	private String bucketName;
	
	private AmazonS3 client;
	
	private static final String delimiter = "/";

	public S3FsClient() {
		client = new AmazonS3Client();
	}
	
	@Override
	public void putInputStream(InputStream in, String... paths) {
		String path = FsSalixService.arrayToDelimitedString(paths, delimiter);
		PutObjectRequest req = new PutObjectRequest(bucketName, path, in, new ObjectMetadata());
		client.putObject(req);
	}

	@Override
	public InputStream getInputStream(String... paths) {
		String path = FsSalixService.arrayToDelimitedString(paths, delimiter);
		S3Object object = getObject(path);
		return object.getObjectContent();
	}
	
	@Override
	public List<String> listFoldersInFolder(String... paths) {
		String path = FsSalixService.arrayToDelimitedString(paths, delimiter);
		ObjectListing objects = listObjectsInFolder(path);
	    return objects.getCommonPrefixes().stream().map(s -> s.substring(0, s.length() - 1)).collect(Collectors.toList());
	}
	
	@Override
	public List<String> listFilesInFolder(String... paths) {
		String path = FsSalixService.arrayToDelimitedString(paths, delimiter);
	    ObjectListing objects = listObjectsInFolder(path);
	    return objects.getObjectSummaries().stream().filter(s -> s.getKey().startsWith(path) && s.getKey().length() > path.length())
	    		.map(s -> s.getKey().substring(path.length() + 1))
	    		.filter(f -> StringUtils.hasText(f) && ! f.endsWith(delimiter))
	    		.collect(Collectors.toList());
	}
	
	private ObjectListing listObjectsInFolder(String prefix) {
	    if (StringUtils.hasText(prefix) && !prefix.endsWith(delimiter)) {
	        prefix += delimiter;
	    }
	    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
	            .withBucketName(bucketName).withPrefix(prefix)
	            .withDelimiter(delimiter);
	    try {
	    	return client.listObjects(listObjectsRequest);
	    } catch(AmazonServiceException ase) {
	    	logger.error("Exception trying to listObjectsInFolder(" + prefix + ")");
	    	throw new RuntimeException(ase);
	    }
	}

	private ObjectListing listAllObjectsInFolder(String prefix) {
	    if (StringUtils.hasText(prefix) && !prefix.endsWith(delimiter)) {
	        prefix += delimiter;
	    }
	    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
	            .withBucketName(bucketName).withPrefix(prefix);
	    try {
	    	return client.listObjects(listObjectsRequest);
	    } catch(AmazonServiceException ase) {
	    	logger.error("Exception trying to listObjectsInFolder(" + prefix + ")");
	    	throw new RuntimeException(ase);
	    }
	}
	
	private S3Object getObject(String key) {
		try {
			return client.getObject(bucketName, key);
		} catch(AmazonServiceException ase) {
			logger.error("Exception trying to getObject(" + key + ")");
	    	throw new RuntimeException(ase);
	    }
	}

	@Override
	public boolean exists(String... paths) {
		String path = FsSalixService.arrayToDelimitedString(paths, delimiter);
		try {
			S3Object object = getObject(path);
			object.close();
			return object != null;
		} catch(Exception ex) {
			return false;
		}
	}

	@Override
	public List<String> listFilesInSubFolders(String... paths) {
		String path = FsSalixService.arrayToDelimitedString(paths, delimiter);
	    ObjectListing objects = listAllObjectsInFolder(path);
	    return objects.getObjectSummaries().stream().filter(s -> s.getKey().startsWith(path) && s.getKey().length() > path.length())
	    		.map(s -> s.getKey().substring(path.length() + 1))
	    		.filter(f -> StringUtils.hasText(f) && ! f.endsWith(delimiter))
	    		.collect(Collectors.toList());
	}

}
