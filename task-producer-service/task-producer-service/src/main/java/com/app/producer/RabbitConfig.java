package com.app.producer;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

	@Bean
	public Queue taskQueue(@Value("${producer.queue}") String name) {
		return new Queue(name, true);
	}
	
	@Bean
	public MessageConverter jsonMessageConverter() {
		return new JacksonJsonMessageConverter();
	}
	
	@Bean
	public Queue deadLetterQueue() {
		return QueueBuilder.durable("tasks.dlq").build();
	}

	@Bean
	public FanoutExchange deadLetterExchange() {
		return new FanoutExchange("dlx");
	}

	@Bean
	public Binding deadLetterBinding() {
		return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange());
	}
}
