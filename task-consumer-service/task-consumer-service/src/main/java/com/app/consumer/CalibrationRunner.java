package com.app.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "processor", name = "calibrate", havingValue = "true")
public class CalibrationRunner implements ApplicationRunner{

	private static final Logger log = LoggerFactory.getLogger(CalibrationRunner.class);
	private final ConfigurableApplicationContext ctx;
	
	public CalibrationRunner(ConfigurableApplicationContext ctx) {
		this.ctx = ctx;
	}
	@Override
	public void run(ApplicationArguments args) throws Exception {
		String input = "payload-calibration-0123456789-0123456789";
		for(int cost = 8; cost <= 13; cost++) {
			BCryptPasswordEncoder enc = new BCryptPasswordEncoder(cost);
			for(int i = 0; i < 10; i++) enc.encode(input);
			int runs = 20;
			long t0 = System.nanoTime();
			for(int i = 0; i < runs; i++) enc.encode(input);
			double ms = (System.nanoTime() - t0) / 1e6 / runs;
			log.info("KALIBRACIJA: cost={} -> {} ms po hashu", cost, String.format("%.1f", ms));
			
		}
		System.exit(SpringApplication.exit(ctx));
	}

}
