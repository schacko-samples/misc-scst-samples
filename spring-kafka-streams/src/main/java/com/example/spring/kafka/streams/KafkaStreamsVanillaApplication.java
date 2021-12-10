package com.example.spring.kafka.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsVanillaApplication {

	static public class CardTransaction {

		UUID uuid;
		UUID userId;
		Long amountCents;

		public CardTransaction() {
		}

		public void setUuid(UUID uuid) {
			this.uuid = uuid;
		}

		public void setUserId(UUID userId) {
			this.userId = userId;
		}

		public void setAmountCents(Long amountCents) {
			this.amountCents = amountCents;
		}

		public UUID getUuid() {
			return uuid;
		}

		public UUID getUserId() {
			return userId;
		}

		public Long getAmountCents() {
			return amountCents;
		}

	}

	static public class UserData {
		UUID id;
		String phoneNumber;
		Integer creditLimitDollars;

		public UserData() {
		}

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
		}

		public String getPhoneNumber() {
			return phoneNumber;
		}

		public void setPhoneNumber(String phoneNumber) {
			this.phoneNumber = phoneNumber;
		}

		public Integer getCreditLimitDollars() {
			return creditLimitDollars;
		}

		public void setCreditLimitDollars(Integer creditLimitDollars) {
			this.creditLimitDollars = creditLimitDollars;
		}
	}

	static class EnrichedCardTransaction {
		CardTransaction transaction;
		UserData user;
		SuspiciousActivity suspiciousActivity;

		public EnrichedCardTransaction() {
		}

		public CardTransaction getTransaction() {
			return transaction;
		}

		public void setTransaction(CardTransaction transaction) {
			this.transaction = transaction;
		}

		public UserData getUser() {
			return user;
		}

		public void setUser(UserData user) {
			this.user = user;
		}

		public SuspiciousActivity getSuspiciousActivity() {
			return suspiciousActivity;
		}

		public void setSuspiciousActivity(SuspiciousActivity suspiciousActivity) {
			this.suspiciousActivity = suspiciousActivity;
		}
	}

	enum SuspiciousActivity {
		TRANSACTION_TOO_BIG,
		FRACTIONAL_PRIMING_TRANSACTIONS;
	}

	static class DetectedFraud {

		EnrichedCardTransaction cardTransaction;
		SuspiciousActivity reasonFlagged;
		//Instant timeFlagged;
		String timeFlagged;

		public DetectedFraud() {
		}

		public EnrichedCardTransaction getCardTransaction() {
			return cardTransaction;
		}

		public void setCardTransaction(EnrichedCardTransaction cardTransaction) {
			this.cardTransaction = cardTransaction;
		}

		public SuspiciousActivity getReasonFlagged() {
			return reasonFlagged;
		}

		public void setReasonFlagged(SuspiciousActivity reasonFlagged) {
			this.reasonFlagged = reasonFlagged;
		}

		public String getTimeFlagged() {
			return timeFlagged;
		}

		public void setTimeFlagged(String timeFlagged) {
			this.timeFlagged = timeFlagged;
		}
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kStreamsConfigs() {
		Map<String, Object> streamsConfiguration = new HashMap<>();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "raw-spring-kafka-stream-1");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "raw-spring-kafka-stream-1-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Specify default (de)serializers for record keys and for record values.

		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.UUID().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the default
		// in order to keep this example interactive.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches.
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		return new KafkaStreamsConfiguration(streamsConfiguration);
	}

	@Bean
	public KStream<UUID, DetectedFraud> kStream(StreamsBuilder kStreamBuilder, Serde<DetectedFraud> detectedFraudSerde,
												Serde<EnrichedCardTransaction> enrichedCardTransactionSerde,
												Serde<UserData> userDataSerde,
												Serde<CardTransaction> cardTransactionSerde) {

		final KStream<UUID, CardTransaction> stream = kStreamBuilder.stream("txn-vanilla",
				Consumed.with(Serdes.UUID(), cardTransactionSerde));

		final KTable<UUID, UserData> userData = kStreamBuilder.table("user-vanilla", Consumed.with(Serdes.UUID(), userDataSerde));

		final KStream<UUID, CardTransaction> uuidCardTransactionKStream = stream.selectKey((uuid, cardTransaction) -> cardTransaction.userId);
		final KStream<UUID, EnrichedCardTransaction> join = uuidCardTransactionKStream
				.join(userData,
						(cardTransaction, user) -> {
							final EnrichedCardTransaction enrichedCardTransaction = new EnrichedCardTransaction();
							enrichedCardTransaction
									.setTransaction(cardTransaction);
							enrichedCardTransaction.setUser(user);
							return enrichedCardTransaction;
						},
						Joined.with(Serdes.UUID(), cardTransactionSerde, userDataSerde));

		final KGroupedStream<UUID, EnrichedCardTransaction> uuidEnrichedCardTransactionKGroupedStream = join.groupByKey(Grouped.with(Serdes.UUID(), enrichedCardTransactionSerde));

		final TimeWindowedKStream<UUID, EnrichedCardTransaction> uuidEnrichedCardTransactionTimeWindowedKStream = uuidEnrichedCardTransactionKGroupedStream.windowedBy(TimeWindows.of(Duration.ofSeconds(60)));

		final KTable<Windowed<UUID>, EnrichedCardTransaction> aggregate = uuidEnrichedCardTransactionTimeWindowedKStream.aggregate(
				() -> null,
				(windowedId, currTx, prevTx) -> checkForFraud(currTx, prevTx),
				Materialized.<UUID, EnrichedCardTransaction, WindowStore<Bytes, byte[]>>as("windowed-agg-txns-store")
						.withKeySerde(Serdes.UUID())
						.withValueSerde(enrichedCardTransactionSerde));

		final KTable<Windowed<UUID>, EnrichedCardTransaction> filter = aggregate.filter((windowedId, enrichedTx) -> enrichedTx.getSuspiciousActivity() != null);

		final KTable<Windowed<UUID>, DetectedFraud> windowedDetectedFraudKTable = filter.mapValues((enrichedTx) -> {
					DetectedFraud detectedFraud = new DetectedFraud();
					detectedFraud.setCardTransaction(enrichedTx);
					detectedFraud.setReasonFlagged(enrichedTx.getSuspiciousActivity());
					detectedFraud.setTimeFlagged(Instant.now().toString());

					return detectedFraud;
				},
				Materialized.<Windowed<UUID>, DetectedFraud, KeyValueStore<Bytes, byte[]>>as("detected-fraud-txns")
						.withKeySerde(WindowedSerdes.timeWindowedSerdeFrom(UUID.class))
						.withValueSerde(detectedFraudSerde));

		final KStream<UUID, DetectedFraud> uuidDetectedFraudKStream = windowedDetectedFraudKTable.toStream((windowedId, detectedFraud) -> windowedId.key());

		uuidDetectedFraudKStream.to("suspected-txn-vanilla", Produced.with(Serdes.UUID(), detectedFraudSerde));

		return uuidDetectedFraudKStream;
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsVanillaApplication.class, args);
	}

	@Bean
	public Serde<DetectedFraud> getDetectedFraudSerde() {
		return new JsonSerde<>(DetectedFraud.class);
	}

	@Bean
	public Serde<EnrichedCardTransaction> getEnrichedCardTransactionSerde() {
		return new JsonSerde<>(EnrichedCardTransaction.class);
	}

	@Bean
	public Serde<UserData> getUserDataSerde() {
		return new JsonSerde<>(UserData.class);
	}

	@Bean
	public Serde<CardTransaction> getCardTransactionSerde() {
		return new JsonSerde<>(CardTransaction.class);
	}

	private static EnrichedCardTransaction checkForFraud(EnrichedCardTransaction currTx, EnrichedCardTransaction prevTx) {
		if (prevTx != null && prevTx.getTransaction() != null && (isSmall(prevTx.getTransaction().getAmountCents()) && isLarge(currTx.getTransaction().getAmountCents()))) {

			EnrichedCardTransaction enrichedCardTransaction = new EnrichedCardTransaction();
			enrichedCardTransaction.setSuspiciousActivity(SuspiciousActivity.FRACTIONAL_PRIMING_TRANSACTIONS);
			return enrichedCardTransaction;
		}

		return currTx;
	}

	private static boolean isSmall(Long amountCents) {
		// Default is < $1
		return amountCents < 100;
	}

	private static boolean isLarge(Long amountCents) {
		// Default is > $20
		return amountCents > 2000;
	}
}
