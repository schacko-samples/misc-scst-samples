package com.example.kafkastreamsvanilla;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
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


	public static void main(String[] args) {

		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

		// Configure the Streams application.
		final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);
		final StreamsBuilder builder = new StreamsBuilder();

		final Serde<CardTransaction> pageViewSerde = getCardTransactionSerde();
		final KStream<UUID, CardTransaction> stream = builder.stream("txn-vanilla", Consumed.with(Serdes.UUID(), pageViewSerde));

		final Serde<UserData> userDataSerde = getUserDataSerde();
		final Serde<EnrichedCardTransaction> enrichedCardTransactionSerde = getEnrichedCardTransactionSerde();

		final KTable<UUID, UserData> userData = builder.table("user-vanilla", Consumed.with(Serdes.UUID(), userDataSerde));

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
						Joined.with(Serdes.UUID(), pageViewSerde, userDataSerde));

		final KGroupedStream<UUID, EnrichedCardTransaction> uuidEnrichedCardTransactionKGroupedStream = join.groupByKey(Grouped.with(Serdes.UUID(), enrichedCardTransactionSerde));

		final TimeWindowedKStream<UUID, EnrichedCardTransaction> uuidEnrichedCardTransactionTimeWindowedKStream = uuidEnrichedCardTransactionKGroupedStream.windowedBy(TimeWindows.of(Duration.ofSeconds(60)));

		final KTable<Windowed<UUID>, EnrichedCardTransaction> aggregate = uuidEnrichedCardTransactionTimeWindowedKStream.aggregate(
				() -> null,
				(windowedId, currTx, prevTx) -> checkForFraud(currTx, prevTx),
				Materialized.<UUID, EnrichedCardTransaction, WindowStore<Bytes, byte[]>>as("windowed-agg-txns-store")
						.withKeySerde(Serdes.UUID())
						.withValueSerde(enrichedCardTransactionSerde));

		final KTable<Windowed<UUID>, EnrichedCardTransaction> filter = aggregate.filter((windowedId, enrichedTx) -> enrichedTx.getSuspiciousActivity() != null);

		final Serde<DetectedFraud> detectedFraudSerde = getDetectedFraudSerde();

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

		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

		streams.cleanUp();

		// Now run the processing topology via `start()` to begin processing its input data.
		streams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static Serde<DetectedFraud> getDetectedFraudSerde() {
		Map<String, Object> serdeProps = new HashMap<>();
		final Serializer<DetectedFraud> detectedFraudSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", DetectedFraud.class);
		detectedFraudSerializer.configure(serdeProps, false);

		final Deserializer<DetectedFraud> detectedFraudDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", DetectedFraud.class);
		detectedFraudDeserializer.configure(serdeProps, false);

		final Serde<DetectedFraud> detectedFraudSerde = Serdes.serdeFrom(detectedFraudSerializer, detectedFraudDeserializer);
		return detectedFraudSerde;
	}

	private static Serde<EnrichedCardTransaction> getEnrichedCardTransactionSerde() {
		Map<String, Object> serdeProps = new HashMap<>();
		final Serializer<EnrichedCardTransaction> enrichedCardTransactionSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", EnrichedCardTransaction.class);
		enrichedCardTransactionSerializer.configure(serdeProps, false);

		final Deserializer<EnrichedCardTransaction> enrichedCardTransactionDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", EnrichedCardTransaction.class);
		enrichedCardTransactionDeserializer.configure(serdeProps, false);

		final Serde<EnrichedCardTransaction> enrichedCardTransactionSerde = Serdes.serdeFrom(enrichedCardTransactionSerializer, enrichedCardTransactionDeserializer);
		return enrichedCardTransactionSerde;
	}

	private static Serde<UserData> getUserDataSerde() {
		Map<String, Object> serdeProps = new HashMap<>();
		final Serializer<UserData> userDataSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", UserData.class);
		userDataSerializer.configure(serdeProps, false);

		final Deserializer<UserData> userDataDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", UserData.class);
		userDataDeserializer.configure(serdeProps, false);

		final Serde<UserData> userDataSerde = Serdes.serdeFrom(userDataSerializer, userDataDeserializer);
		return userDataSerde;
	}

	private static Serde<CardTransaction> getCardTransactionSerde() {
		Map<String, Object> serdeProps = new HashMap<>();

		final Serializer<CardTransaction> pageViewSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", CardTransaction.class);
		pageViewSerializer.configure(serdeProps, false);

		final Deserializer<CardTransaction> pageViewDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", CardTransaction.class);
		pageViewDeserializer.configure(serdeProps, false);

		final Serde<CardTransaction> pageViewSerde = Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer);
		return pageViewSerde;
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

	static Properties getStreamsConfiguration(final String bootstrapServers) {
		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name.  The name must be unique in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// Specify default (de)serializers for record keys and for record values.

		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.UUID().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the default
		// in order to keep this example interactive.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches.
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		return streamsConfiguration;
	}
}
