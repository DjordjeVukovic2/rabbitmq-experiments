package com.app.producer;

import java.util.Map;
import java.util.UUID;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PublishController {

	private final RabbitTemplate rabbit;
	private final String queue;
	private final MessagePostProcessor deliveryMode;
	
	public PublishController(RabbitTemplate rabbit,
								@Value("${producer.queue}") String queue,
								@Value("${producer.persistent:true}") boolean persistent) {
		this.rabbit = rabbit;
		this.queue = queue;
		MessageDeliveryMode mode = persistent ? MessageDeliveryMode.PERSISTENT : MessageDeliveryMode.NON_PERSISTENT;
		this.deliveryMode = message -> {
			message.getMessageProperties().setDeliveryMode(mode);
			return message;
		};
	}
	
	private int sendAll(RabbitOperations ops, int count, int poison, MessagePostProcessor mode) {
		int step = poison > 0 ? Math.max(1, count / poison) : 0;
		int sentPoison = 0;
		for (int i = 0; i < count; i++) {
			boolean bad = step > 0 && i % step == 0 && sentPoison < poison;
			if (bad) sentPoison++;
			String payload = (bad ? "POISON-" : "payload-") + i + "-" + UUID.randomUUID();
			TaskMessage msg = new TaskMessage(UUID.randomUUID(), payload, System.currentTimeMillis());
			ops.convertAndSend(queue, msg, mode);
		}
		return sentPoison;
	}

	@PostMapping("/publish")
	public Map<String, Object> publish(@RequestParam int count,
	                                   @RequestParam(defaultValue = "0") int poison,
	                                   @RequestParam(required = false) Boolean persistent,
	                                   @RequestParam(defaultValue = "false") boolean confirm){
		MessagePostProcessor chosen = this.deliveryMode;
		if (persistent != null) {
			MessageDeliveryMode m = persistent ? MessageDeliveryMode.PERSISTENT : MessageDeliveryMode.NON_PERSISTENT;
			chosen = message -> {
				message.getMessageProperties().setDeliveryMode(m);
				return message;
			};
		}
		final MessagePostProcessor mode = chosen;
		long start = System.currentTimeMillis();
		int sentPoison;
		if (confirm) {
			sentPoison = rabbit.invoke(ops -> {
				int sp = sendAll(ops, count, poison, mode);
				ops.waitForConfirmsOrDie(300_000);
				return sp;
			});
		} else {
			sentPoison = sendAll(rabbit, count, poison, mode);
		}
		long elapsed = System.currentTimeMillis() - start;
		return Map.of("published", count, "poison", sentPoison, "confirm", confirm, "elapsedMs", elapsed);
	}
}
