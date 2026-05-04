package be.nabu.eai.server;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

public class HeartbeatOTLPReporter {
	private static final String METER_NAME = "be.nabu.metrics";
	private static final String METRIC_DRIFT = "nabu.heartbeat.drift_ms";

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final SdkMeterProvider meterProvider;
	private final ScheduledExecutorService scheduler;
	private final LongGauge driftGauge;
	private final long intervalMs;

	public HeartbeatOTLPReporter(String serverGroup, String serverName, String otelEndpoint, MetricsOTLPProcessor.OTLPProtocol protocol, long intervalMs) {
		this.intervalMs = Math.max(100, intervalMs);
		Resource resource = Resource.getDefault().toBuilder()
				.put(AttributeKey.stringKey("service.name"), "nabu")
				.put(AttributeKey.stringKey("service.namespace"), serverGroup)
				.put(AttributeKey.stringKey("service.instance.id"), serverName)
				.build();

		SdkMeterProvider createdProvider = null;
		try {
			MetricExporter exporter = buildExporter(otelEndpoint, protocol);
			createdProvider = SdkMeterProvider.builder()
					.setResource(resource)
					.registerMetricReader(PeriodicMetricReader.builder(exporter)
							.setInterval(Duration.ofMillis(this.intervalMs))
							.build())
					.build();
		}
		catch (Exception e) {
			logger.error("Failed to initialize OTLP heartbeat exporter for {} {}", protocol, otelEndpoint, e);
		}

		this.meterProvider = createdProvider == null
				? SdkMeterProvider.builder().setResource(resource).build()
				: createdProvider;

		Meter meter = meterProvider.get(METER_NAME);
		this.driftGauge = meter.gaugeBuilder(METRIC_DRIFT)
				.ofLongs()
				.setUnit("ms")
				.build();

		this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable runnable) {
				Thread thread = new Thread(runnable, "otlp-heartbeat");
				thread.setDaemon(true);
				return thread;
			}
		});

		start();
	}

	private MetricExporter buildExporter(String otelEndpoint, MetricsOTLPProcessor.OTLPProtocol protocol) {
		switch (protocol) {
		case HTTP:
			return OtlpHttpMetricExporter.builder()
					.setEndpoint(otelEndpoint)
					.setTimeout(Duration.ofSeconds(15))
					.build();
		case GRPC:
		default:
			return OtlpGrpcMetricExporter.builder()
					.setEndpoint(otelEndpoint)
					.setTimeout(Duration.ofSeconds(15))
					.build();
		}
	}

	private void start() {
		final long intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMs);
		long startNanos = System.nanoTime();
		final long[] lastRunNanos = new long[] { startNanos };
		scheduler.scheduleWithFixedDelay(() -> {
			long now = System.nanoTime();
			long expected = lastRunNanos[0] + intervalNanos;
			long driftNanos = now - expected;
			if (driftNanos < 0) {
				driftNanos = 0;
			}
			driftGauge.set(TimeUnit.NANOSECONDS.toMillis(driftNanos));
			lastRunNanos[0] = now;
		}, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		scheduler.shutdownNow();
		meterProvider.shutdown();
	}
}
