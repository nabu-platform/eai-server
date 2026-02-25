package be.nabu.eai.server;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.util.MetricStatistics;
import be.nabu.libs.metrics.core.api.SinkStatistics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

/**
 * It is impossible at this level to manipulate the timestamps, the data is always timestamped as "now"
 * To start tweaking this we would need to go much lower level
 */
public class MetricsOTLPProcessor implements be.nabu.libs.events.api.EventHandler<MetricStatistics, Void> {

	public enum OTLPProtocol {
		GRPC,
		HTTP
	}
	
	private SdkMeterProvider meterProvider;
	private Meter meter;
	private Logger logger = LoggerFactory.getLogger(getClass());

	private static final AttributeKey<String> ATTR_SERVICE_NAME = AttributeKey.stringKey("service.name");
	private static final AttributeKey<String> ATTR_CATEGORY = AttributeKey.stringKey("category");

	public MetricsOTLPProcessor(String serverGroup, String serverName, String otelEndpoint, OTLPProtocol protocol) {
		// initialize as no op until the actual becomes available
		this.meter = OpenTelemetry.noop().getMeter("be.nabu.metrics");
		Resource resource = Resource.getDefault().toBuilder()
				// note that this is the service name of the entire resource which should end up as the "service" tag in datadog
				// this MUST match the "service" configured in the log pickup configuration for correlation:
//				logs:
//					  - type: file
//					    path: /var/log/nabu-events.jsonl
//					    service: nabu
//					    source: java
//					    # Adding tags helps you filter logs across different clusters or logical server groups
//					    tags:
//					      - "env:dev"

				.put(AttributeKey.stringKey("service.name"), "nabu")
				.put(AttributeKey.stringKey("service.namespace"), serverGroup)
				.put(AttributeKey.stringKey("service.instance.id"), serverName)
				.build();
		switch(protocol) {
			case HTTP:
				MetricExporter httpExporter = OtlpHttpMetricExporter.builder()
					.setEndpoint(otelEndpoint)
					.setTimeout(Duration.ofSeconds(15)) 
					.build();
		
				this.meterProvider = SdkMeterProvider.builder()
						.setResource(resource)
						.registerMetricReader(PeriodicMetricReader.builder(httpExporter)
								.setInterval(Duration.ofSeconds(30))
								.build())
						.build();
				this.meter = meterProvider.get("be.nabu.metrics");
			default:
				CompletableFuture.runAsync(() -> {
					try {
						MetricExporter grpcExporter = OtlpGrpcMetricExporter.builder()
								.setEndpoint(otelEndpoint)
								.setTimeout(Duration.ofSeconds(15)) 
								.build();
						
						this.meterProvider = SdkMeterProvider.builder()
								.setResource(resource)
								.registerMetricReader(PeriodicMetricReader.builder(grpcExporter)
										.setInterval(Duration.ofSeconds(30))
										.build())
								.build();
						
						this.meter = meterProvider.get("be.nabu.metrics");
					}
					catch (Exception e) {
						logger.error("Failed to connect gRPC otel: " + otelEndpoint);
					}
				});
		}
	}

	@Override
	public Void handle(MetricStatistics event) {
		if (event == null || event.getStatistics() == null) {
			return null;
		}

		SinkStatistics stats = event.getStatistics();
		Attributes attrs = Attributes.of(ATTR_SERVICE_NAME, event.getId(), ATTR_CATEGORY, event.getCategory());
		
		recordValue("nabu.metrics.min", stats.getMinimum().getValue(), attrs);
		recordValue("nabu.metrics.max", stats.getMaximum().getValue(), attrs);
		recordValue("nabu.metrics.ema", stats.getExponentialAverage(), attrs);
		recordValue("nabu.metrics.cma", stats.getCumulativeAverage(), attrs);

		meter.counterBuilder("nabu.metrics.count")
				.build()
				.add(stats.getAmountOfDataPoints(), attrs);

		meter.counterBuilder("nabu.metrics.total")
				.build()
				.add(stats.getTotal(), attrs);

		return null;
	}

	private void recordValue(String name, double value, Attributes attrs) {
		meter.histogramBuilder(name)
				.build()
				.record(value, attrs);
	}

	public void stop() {
		meterProvider.shutdown();
	}
}