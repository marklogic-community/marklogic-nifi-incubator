package org.apache.nifi.marklogic.processor;

import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.client.ext.spring.SimpleDatabaseClientProvider;
import com.marklogic.junit5.spring.SimpleTestConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * SimpleTestConfig looks for mlTestRestPort by default, but we only have mlRestPort. So this overrides it to
 * use mlRestPort instead as the REST port number.
 */
@Configuration
@PropertySource(
	value = {"file:gradle.properties", "file:gradle-local.properties"},
	ignoreResourceNotFound = true
)
public class TestConfigDHF4 extends SimpleTestConfig {

	@Value("${mlRestPortDHF4:0}")	
	private Integer restPort;
	
	@Value("${mlDatabaseDHF4:0}")	
	private String database;

	@Override
	public Integer getRestPort() {
		return restPort;
	}
	
	public String getDatabase() {
		return database;
	}
	
}
