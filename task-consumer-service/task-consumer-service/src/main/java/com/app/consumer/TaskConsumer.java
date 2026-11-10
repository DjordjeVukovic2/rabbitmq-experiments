package com.app.consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
public class TaskConsumer {

	private static final Logger log = LoggerFactory.getLogger(TaskConsumer.class);
	
	private final BCryptPasswordEncoder encoder;
	private final Counter processed;
	private final Timer hashTimer;
	private final Timer waitTimer;
	private final AtomicLong localCount = new AtomicLong();
	
	public TaskConsumer(@Value("${processor.bcrypt-cost}") int cost,
								MeterRegistry registry) {
		
		this.encoder = new BCryptPasswordEncoder(cost);
		this.processed = registry.counter("tasks.processed");
		this.hashTimer = registry.timer("tasks.hash");
		this.waitTimer = registry.timer("tasks.queue_wait");
		log.info("Consumer je pokrenut, BCrypt cost = {}", cost);
	}
	
	@RabbitListener(queues = "${processor.queue}")
	public void handle(TaskMessage msg) {
		
		if (msg.payload().startsWith("POISON")) {
			throw new IllegalStateException("Neispravna poruka: " + msg.id());
		}
		waitTimer.record(System.currentTimeMillis() - msg.sentAt(), TimeUnit.MILLISECONDS);
		
		long t0 = System.nanoTime();
		encoder.encode(msg.payload());
		hashTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
		
		processed.increment();
		
		long n = localCount.incrementAndGet();
		if(n % 5000 == 0) {
			log.info("Ova instanca je obradila {} poruka", n);
		}
	}
}
