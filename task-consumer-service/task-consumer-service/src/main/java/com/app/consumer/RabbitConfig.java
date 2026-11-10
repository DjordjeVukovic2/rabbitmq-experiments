package com.app.consumer;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

	@Bean
	public Queue taskQueue(@Value("${processor.queue}") String name) {
		return new Queue(name, true);
	}
	
	@Bean
	public MessageConverter jacksonMessageConverter() {
		JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
		converter.setAlwaysConvertToInferredType(true);
		return converter;
	}
}
