package org.lukosan.salix.autoconfigure;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lukosan.salix.SalixService;
import org.lukosan.salix.s3.S3SalixService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class S3AutoConfiguration {

	@Configuration
	public static class SalixS3Configuration {
		
		private static final Log logger = LogFactory.getLog(SalixS3Configuration.class);
		
		@PostConstruct
		public void postConstruct() {
			if(logger.isInfoEnabled())
				logger.info("PostConstruct " + getClass().getSimpleName());
		}
		
		@Bean
		public SalixService s3Service() {
			return new S3SalixService();
		}
		
	}
	
}