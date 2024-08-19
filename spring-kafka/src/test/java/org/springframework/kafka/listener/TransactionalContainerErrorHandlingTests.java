package org.springframework.kafka.listener;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.verify;

@SpringJUnitConfig
@EmbeddedKafka(
		topics = {TransactionalContainerErrorHandlingTests.TOPIC},
		partitions = 1,
		brokerProperties = {"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"})
public class TransactionalContainerErrorHandlingTests {

	static final String TOPIC = "txTopic1";
	private static final String POISON_PILL = "poison-pill";
	private static final int MAX_ATTEMPT_COUNT = 5;

	public static class TestStringDeserializer implements Deserializer<String> {

		public TestStringDeserializer() {
		}

		@Override
		public String deserialize(String topic, byte[] data) {
			String s = new String(data, StandardCharsets.UTF_8);
			if (s.equals(POISON_PILL)) {
				throw new RuntimeException("Deserialization failed due to poison pill");
			}
			return s;
		}
	}

	/**
	 * Exceptions of this type should eventually be recovered with a DeadLetterPublishingRecoverer.
	 */
	static class SendMessageToDltException extends RuntimeException {
	}

	/**
	 * Exceptions of this type should eventually be recovered by just ignoring them.
	 */
	static class SkipMessageException extends RuntimeException {
	}

	/**
	 * An error handler that always throws exceptions.
	 */
	static class ThrowingErrorHandler implements CommonErrorHandler {
		public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
										 MessageListenerContainer container, boolean batchListener) {
			throw new RuntimeException("Will not handle this error", thrownException);
		}

		public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
								 MessageListenerContainer container) {
			throw new RuntimeException("Will not handle this error", thrownException);
		}

		public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
									MessageListenerContainer container) {
			throw new RuntimeException("Will not handle this error", thrownException);
		}

		public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data,
								Consumer<?, ?> consumer, MessageListenerContainer container, Runnable invokeListener) {
			throw new RuntimeException("Will not handle this error", thrownException);
		}

		@Override
		public boolean seeksAfterHandling() {
			return true;
		}
	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		private EmbeddedKafkaBroker broker;


		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> consumerFactory,
				CommonErrorHandler commonErrorHandler,
				AfterRollbackProcessor<Integer, String> afterRollbackProcessor,
				KafkaTransactionManager<?, ?> kafkaTransactionManager) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setCommonErrorHandler(commonErrorHandler);
			factory.setAfterRollbackProcessor(afterRollbackProcessor);
			factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
			return factory;
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "false", this.broker);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TestStringDeserializer.class);
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory() {
			DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(this.broker));
			producerFactory.setTransactionIdPrefix("tx-");
			return producerFactory;
		}

		@Bean
		public KafkaTransactionManager<?, ?> kafkaTransactionManager(ProducerFactory<?, ?> producerFactory) {
			return new KafkaTransactionManager<>(producerFactory);
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		KafkaListeners kafkaListeners(CountDownLatch successLatch,
									  AtomicInteger errorCounter) {
			return new KafkaListeners(successLatch, errorCounter);
		}

		@Bean
		BackOff backOff() {
			return new FixedBackOff(10, MAX_ATTEMPT_COUNT - 1);
		}

		@Bean
		CommonContainerStoppingErrorHandler commonContainerStoppingErrorHandler() {
			return Mockito.spy(new CommonContainerStoppingErrorHandler());
		}

		@Bean
		ConsumerRecordRecoverer skippingRecoverer() {
			return (record, exception) -> {
				LogFactory.getLog(getClass()).error("Skipping record " + record);
			};
		}

		@Bean
		ConsumerRecordRecoverer deadLetterPublishingRecoverer(KafkaTemplate<?, ?> kafkaTemplate) {
			return new DeadLetterPublishingRecoverer(kafkaTemplate);
		}

		@Bean
		ConsumerRecordRecoverer noRecoveryRecoverer() {
			return (record, exception) -> {
				throw new RuntimeException("Will not recover from " + exception.getClass().getName(), exception);
			};
		}

		@Bean
		CommonErrorHandler commonErrorHandler(
				CommonContainerStoppingErrorHandler commonContainerStoppingErrorHandler,
				ConsumerRecordRecoverer noRecoveryRecoverer) {

			// Two options here, either the default error handler throws exceptions, or the recoverer throws exceptions.
			// Results seems to be the same with both options.

			//CommonDelegatingErrorHandler errorHandler = new CommonDelegatingErrorHandler(new DefaultErrorHandler(noRecoveryRecoverer, new FixedBackOff(1, 0)));
			CommonDelegatingErrorHandler errorHandler = new CommonDelegatingErrorHandler(new ThrowingErrorHandler());
			errorHandler.addDelegate(RecordDeserializationException.class, commonContainerStoppingErrorHandler);
			return errorHandler;
		}

		@Bean
		AfterRollbackProcessor<?, ?> afterRollbackProcessor(
				KafkaTemplate<?, ?> kafkaTemplate,
				BackOff backOff,
				ConsumerRecordRecoverer deadLetterPublishingRecoverer,
				ConsumerRecordRecoverer skippingRecoverer,
				ConsumerRecordRecoverer noRecoveryRecoverer) {
			return new DefaultAfterRollbackProcessor<>((ConsumerRecordRecoverer) (record, exception) -> {

				// A poor man's variant of CommonDelegatingErrorHandler, but in the form of a AfterRollbackProcessor
				if (NestedExceptionUtils.getMostSpecificCause(exception) instanceof SkipMessageException) {
					skippingRecoverer.accept(record, exception);
				} else if (NestedExceptionUtils.getMostSpecificCause(exception) instanceof SendMessageToDltException) {
					deadLetterPublishingRecoverer.accept(record, exception);
				} else {
					noRecoveryRecoverer.accept(record, exception);
				}
			}, backOff, kafkaTemplate, true);
		}

		@Bean
		CountDownLatch successLatch() {
			return new CountDownLatch(1);
		}

		@Bean
		AtomicInteger errorCounter() {
			return new AtomicInteger();
		}
	}

	static class KafkaListeners {

		private final CountDownLatch successLatch;
		private final AtomicInteger errorCounter;

		KafkaListeners(CountDownLatch successLatch, AtomicInteger errorCounter) {
			this.successLatch = successLatch;
			this.errorCounter = errorCounter;
		}

		@KafkaListener(topics = TransactionalContainerErrorHandlingTests.TOPIC)
		void consumeMessage(String message) {
			if (message.startsWith("error")) {
				errorCounter.incrementAndGet();
				if (message.equals("error-skip")) {
					throw new SkipMessageException();
				}
				if (message.equals("error-dlt")) {
					throw new SendMessageToDltException();
				}
				throw new RuntimeException("application level error");
			} else {
				successLatch.countDown();
			}
		}
	}

	@Test
	@DirtiesContext
	void testDeserializationError(@Autowired KafkaTemplate<Integer, String> kafkaTemplate,
								  @Autowired CommonContainerStoppingErrorHandler commonContainerStoppingErrorHandler,
								  @Autowired CountDownLatch successLatch,
								  @Autowired AtomicInteger errorCounter) throws InterruptedException {

		kafkaTemplate.executeInTransaction(kafkaOperations -> {
			kafkaOperations.send(TOPIC, 1, POISON_PILL);
			kafkaOperations.send(TOPIC, 2, "error");
			kafkaOperations.send(TOPIC, 3, "data");
			return null;
		});
		await().atMost(10, TimeUnit.SECONDS).untilAsserted(
				() -> verify(commonContainerStoppingErrorHandler).handleOtherException(any(), any(), any(), anyBoolean()));
		assertThat(errorCounter).hasValue(0);
		assertThat(successLatch.await(2, TimeUnit.SECONDS)).isFalse();
	}

	@Test
	@DirtiesContext
	void testApplicationErrorNotRecovered(@Autowired KafkaTemplate<Integer, String> kafkaTemplate,
										  @Autowired CountDownLatch successLatch,
										  @Autowired AtomicInteger errorCounter) throws InterruptedException {

		kafkaTemplate.executeInTransaction(kafkaOperations -> {
			kafkaOperations.send(TOPIC, 1, "error");
			kafkaOperations.send(TOPIC, 2, "data");
			return null;
		});

		await().atMost(10, TimeUnit.SECONDS).untilAsserted(
				() -> assertThat(errorCounter).hasValue(MAX_ATTEMPT_COUNT));
		assertThat(successLatch.await(2, TimeUnit.SECONDS)).isFalse();
	}

	@Test
	@DirtiesContext
	void testApplicationErrorSkip(@Autowired KafkaTemplate<Integer, String> kafkaTemplate,
								  @Autowired CountDownLatch successLatch,
								  @Autowired AtomicInteger errorCounter) throws InterruptedException {

		kafkaTemplate.executeInTransaction(kafkaOperations -> {
			kafkaOperations.send(TOPIC, 1, "error-skip");
			kafkaOperations.send(TOPIC, 2, "data");
			return null;
		});
		await().atMost(1000, TimeUnit.SECONDS).untilAsserted(
				() -> assertThat(errorCounter).hasValue(MAX_ATTEMPT_COUNT));
		assertThat(successLatch.await(500, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	@DirtiesContext
	void testApplicationErrorDlt(@Autowired KafkaTemplate<Integer, String> kafkaTemplate,
								 @Autowired CountDownLatch successLatch,
								 @Autowired AtomicInteger errorCounter) throws InterruptedException {

		kafkaTemplate.executeInTransaction(kafkaOperations -> {
			kafkaOperations.send(TOPIC, 1, "error-dlt");
			kafkaOperations.send(TOPIC, 2, "data");
			return null;
		});
		await().atMost(10, TimeUnit.SECONDS).untilAsserted(
				() -> assertThat(errorCounter).hasValue(MAX_ATTEMPT_COUNT));
		assertThat(successLatch.await(2, TimeUnit.SECONDS)).isTrue();
	}
}
