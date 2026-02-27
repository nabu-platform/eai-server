/*
 * Copyright (C) 2015 Alexander Verbruggen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */
package be.nabu.eai.server;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.ComplexEventImpl;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.SdkLoggerProviderBuilder;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OTLPLogger implements EventHandler<Object, Void> {
	private static final Duration OTEL_TIMEOUT = Duration.ofSeconds(15);
	private static final String INSTRUMENTATION_NAME = "be.nabu.eai.events";
	private static final String OTEL_SERVICE_NAME = "nabu";
	private final EventSeverity otlpSeverity = EventSeverity.valueOf(System.getProperty("otlpSeverity", "INFO"));
	private static final Map<String, String> MAPPINGS = Map.ofEntries(
		Map.entry("artifactId", "service.name"),
		Map.entry("serverGroup", "service.namespace"),
		Map.entry("serverName", "service.instance.id"),
		Map.entry("serverHost", "host.name"),
		Map.entry("imageEnvironment", "deployment.environment.name"),
		Map.entry("conversationId", "trace_id"),
		Map.entry("correlationId", "event.context.id"),
		Map.entry("narrativeId", "business.process.id"),
		Map.entry("sessionId", "session.id"),
		Map.entry("alias", "user.name"),
		Map.entry("authenticationId", "user.id"),
		Map.entry("deviceId", "device.id"),
		Map.entry("externalId", "event.reference"),
		Map.entry("origin", "event.provider"),
		Map.entry("eventName", "event.name"),
		Map.entry("eventCategory", "event.category"),
		Map.entry("duration", "event.duration"),
		Map.entry("stacktrace", "exception.stacktrace"),
		Map.entry("action", "event.action"),
		Map.entry("code", "event.code"),
		Map.entry("started", "event.start_time"),
		Map.entry("stopped", "event.end_time"),
		Map.entry("timezone", "process.timezone"),
		Map.entry("sourceIp", "client.address"),
		Map.entry("sourceHost", "client.socket.address"),
		Map.entry("sourcePort", "client.port"),
		Map.entry("destinationIp", "server.address"),
		Map.entry("destinationHost", "server.socket.address"),
		Map.entry("destinationPort", "server.port"),
		Map.entry("applicationProtocol", "network.protocol.name"),
		Map.entry("networkProtocol", "network.type"),
		Map.entry("transportProtocol", "network.transport"),
		Map.entry("sizeIn", "network.io.bytes.receive"),
		Map.entry("sizeOut", "network.io.bytes.transmit"),
		Map.entry("imageVersion", "service.version"),
		Map.entry("imageName", "container.image.name"),
		Map.entry("sourceLongitude", "client.geo.location.lon"),
		Map.entry("sourceLatitude", "client.geo.location.lat"),
		Map.entry("destinationLongitude", "server.geo.location.lon"),
		Map.entry("destinationLatitude", "server.geo.location.lat"),
		Map.entry("impersonatorId", "user.impersonator.id"),
		Map.entry("impersonator", "user.impersonator.name"),
		Map.entry("requestUri", "url.full"),
		Map.entry("method", "http.request.method"),
		Map.entry("userAgent", "user_agent.original"),
		Map.entry("language", "browser.language"),
		Map.entry("bot", "user_agent.bot"),
		Map.entry("responseCode", "http.response.status_code"),
		Map.entry("severity", "severity_text"),
		Map.entry("fileUri", "file.path"),
		Map.entry("fileSize", "file.size")
	);
	private final org.slf4j.Logger logger = LoggerFactory.getLogger(getClass());
	private final Server server;
	private final String host;
	private final SdkTracerProvider tracerProvider;
	private final SdkLoggerProvider loggerProvider;
	private final Tracer tracer;
	private final io.opentelemetry.api.logs.Logger otelLogger;

	public OTLPLogger(Server server, String otelEndpoint, MetricsOTLPProcessor.OTLPProtocol protocol) {
		this.server = server;
		String detectedHost = null;
		try {
			detectedHost = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e) {
			// ignore
		}
		this.host = detectedHost;

		Resource resource = Resource.getDefault()
			.toBuilder()
			.put(AttributeKey.stringKey("service.name"), OTEL_SERVICE_NAME)
			.put(AttributeKey.stringKey("service.namespace"), server.getRepository().getGroup())
			.put(AttributeKey.stringKey("service.instance.id"), server.getRepository().getName())
			.build();

		SpanExporter spanExporter = null;
		LogRecordExporter logExporter = null;
		try {
			switch (protocol) {
				case HTTP:
					spanExporter = OtlpHttpSpanExporter.builder()
						.setEndpoint(otelEndpoint)
						.setTimeout(OTEL_TIMEOUT)
						.build();
					logExporter = OtlpHttpLogRecordExporter.builder()
						.setEndpoint(otelEndpoint)
						.setTimeout(OTEL_TIMEOUT)
						.build();
				break;
				case GRPC:
				default:
					spanExporter = OtlpGrpcSpanExporter.builder()
						.setEndpoint(otelEndpoint)
						.setTimeout(OTEL_TIMEOUT)
						.build();
					logExporter = OtlpGrpcLogRecordExporter.builder()
						.setEndpoint(otelEndpoint)
						.setTimeout(OTEL_TIMEOUT)
						.build();
				break;
			}
		}
		catch (Exception e) {
			logger.error("Failed to initialize OTLP exporters for {} {}", protocol, otelEndpoint, e);
		}

		SdkTracerProviderBuilder tracerBuilder = SdkTracerProvider.builder()
			.setResource(resource);
		if (spanExporter != null) {
			tracerBuilder.addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build());
		}
		this.tracerProvider = tracerBuilder.build();

		SdkLoggerProviderBuilder loggerBuilder = SdkLoggerProvider.builder()
			.setResource(resource);
		if (logExporter != null) {
			loggerBuilder.addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build());
		}
		this.loggerProvider = loggerBuilder.build();

		if (spanExporter != null && logExporter != null) {
			logger.info("OTLP logger initialized using {} at {}", protocol, otelEndpoint);
		}
		else {
			logger.warn("OTLP logger initialized without exporters using {} at {}", protocol, otelEndpoint);
		}

		this.tracer = tracerProvider.get(INSTRUMENTATION_NAME);
		this.otelLogger = loggerProvider.get(INSTRUMENTATION_NAME);
	}

	@Override
	public Void handle(Object event) {
		if (event instanceof ComplexEventImpl) {
			ComplexEventImpl complexEvent = (ComplexEventImpl) event;
			complexEvent.setServerGroup(server.getRepository().getGroup());
			complexEvent.setServerName(server.getRepository().getName());
			complexEvent.setServerHost(host);
			if (complexEvent.getTimezone() == null) {
				complexEvent.setTimezone(TimeZone.getDefault());
			}
			if (complexEvent.getDuration() == null
					&& complexEvent.getStarted() != null
					&& complexEvent.getStopped() != null) {
				complexEvent.setDuration(complexEvent.getStopped().getTime() - complexEvent.getStarted().getTime());
			}
		}

		ComplexContent wrapped = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(event);
		if (wrapped == null) {
			return null;
		}

		EventSeverity severity = getEventSeverity(wrapped.get("severity"));
		if (severity != null && severity.ordinal() < otlpSeverity.ordinal()) {
			return null;
		}

		Attributes attributes = buildAttributes(wrapped);
		Instant started = toInstant(wrapped.get("started"));
		Instant stopped = toInstant(wrapped.get("stopped"));
		String traceId = getTraceId(wrapped);

		if (started != null && stopped != null && traceId != null) {
			String spanName = getSpanName(wrapped);
			SpanBuilder spanBuilder = tracer.spanBuilder(spanName)
				.setStartTimestamp(started.toEpochMilli(), TimeUnit.MILLISECONDS)
				.setAllAttributes(attributes);

			Context parentContext = getParentContext(wrapped);
			if (parentContext != null) {
				spanBuilder.setParent(parentContext);
			}

			Span span = spanBuilder.startSpan();
			applyStatus(span, wrapped);
			span.end(stopped.toEpochMilli(), TimeUnit.MILLISECONDS);
		}
		else {
			LogRecordBuilder logBuilder = otelLogger.logRecordBuilder();
			logBuilder.setAllAttributes(attributes);
			logBuilder.setSeverity(mapSeverity(wrapped));
			String severityText = getSeverityText(wrapped.get("severity"));
			if (severityText != null) {
				logBuilder.setSeverityText(severityText);
			}

			String body = getLogBody(wrapped);
			if (body != null) {
				logBuilder.setBody(body);
			}

			Instant timestamp = started != null ? started : stopped;
			if (timestamp != null) {
				logBuilder.setTimestamp(timestamp);
			}

			Context parentContext = getParentContext(wrapped);
			if (parentContext != null) {
				logBuilder.setContext(parentContext);
			}

			logBuilder.emit();
		}

		return null;
	}

	public void stop() {
		try {
			loggerProvider.shutdown();
		}
		catch (Exception e) {
			logger.warn("Failed to shutdown OTLP logger", e);
		}
		try {
			tracerProvider.shutdown();
		}
		catch (Exception e) {
			logger.warn("Failed to shutdown OTLP tracer", e);
		}
	}

	private Attributes buildAttributes(ComplexContent wrapped) {
		AttributesBuilder builder = Attributes.builder();
		for (Map.Entry<String, String> entry : MAPPINGS.entrySet()) {
			Object value = wrapped.get(entry.getKey());
			if (value == null) {
				continue;
			}
			putAttribute(builder, entry.getValue(), value);
		}

		Object reason = wrapped.get("reason");
		Object message = wrapped.get("message");
		if (reason == null && message != null) {
			putAttribute(builder, "body", message);
		}
		else if (reason != null) {
			putAttribute(builder, "event.message_template", message);
			putAttribute(builder, "body", reason);
		}

		Object parentId = wrapped.get("parentId");
		if (parentId != null) {
			putAttribute(builder, "parent_id", parentId);
		}

		EventSeverity severity = getEventSeverity(wrapped.get("severity"));
		if (severity != null) {
			builder.put(AttributeKey.longKey("severity_number"), severity.getOtelLevel());
			builder.put(AttributeKey.stringKey("severity_text"), severity.name());
		}

		return builder.build();
	}

	private String getSpanName(ComplexContent wrapped) {
		String artifactId = getStringValue(wrapped.get("artifactId"));
		if (artifactId != null) {
			return artifactId;
		}
		String eventName = getStringValue(wrapped.get("eventName"));
		if (eventName != null) {
			return eventName;
		}
		String action = getStringValue(wrapped.get("action"));
		if (action != null) {
			return action;
		}
		String code = getStringValue(wrapped.get("code"));
		if (code != null) {
			return code;
		}
		return "event";
	}

	private String getLogBody(ComplexContent wrapped) {
		Object reason = wrapped.get("reason");
		if (reason != null) {
			return reason.toString();
		}
		Object message = wrapped.get("message");
		if (message != null) {
			return message.toString();
		}
		String eventName = getStringValue(wrapped.get("eventName"));
		if (eventName != null) {
			return eventName;
		}
		return null;
	}

	private Context getParentContext(ComplexContent wrapped) {
		String traceId = getTraceId(wrapped);
		String parentId = getStringValue(wrapped.get("parentId"));
		if (traceId == null || parentId == null) {
			return null;
		}
		if (!TraceId.isValid(traceId) || !SpanId.isValid(parentId)) {
			logger.debug("Invalid trace/span id: traceId={}, parentId={}", traceId, parentId);
			return null;
		}
		SpanContext spanContext = SpanContext.createFromRemoteParent(traceId, parentId, TraceFlags.getSampled(), TraceState.getDefault());
		return Context.root().with(Span.wrap(spanContext));
	}

	private String getTraceId(ComplexContent wrapped) {
		String traceId = getStringValue(wrapped.get("correlationId"));
		if (traceId == null) {
			return null;
		}
		if (TraceId.isValid(traceId)) {
			return traceId;
		}
		int delimiter = traceId.lastIndexOf(':');
		if (delimiter > -1 && delimiter < traceId.length() - 1) {
			String candidate = traceId.substring(delimiter + 1);
			if (TraceId.isValid(candidate)) {
				return candidate;
			}
		}
		return null;
	}

	private Severity mapSeverity(ComplexContent wrapped) {
		EventSeverity severity = getEventSeverity(wrapped.get("severity"));
		if (severity == null) {
			return Severity.INFO;
		}

		return getSeverityFromOtelLevel(severity.getOtelLevel());
	}

	private void applyStatus(Span span, ComplexContent wrapped) {
		EventSeverity severity = getEventSeverity(wrapped.get("severity"));
		if (severity != null && severity.getOtelLevel() >= 17) {
			span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
		}
	}

	private EventSeverity getEventSeverity(Object severityObject) {
		if (severityObject instanceof EventSeverity) {
			return (EventSeverity) severityObject;
		}
		if (severityObject == null) {
			return null;
		}
		try {
			return EventSeverity.valueOf(severityObject.toString().toUpperCase());
		}
		catch (IllegalArgumentException e) {
			logger.debug("Unknown severity: {}", severityObject);
			return null;
		}
	}

	private String getSeverityText(Object severityObject) {
		EventSeverity severity = getEventSeverity(severityObject);
		if (severity != null) {
			return severity.name();
		}
		return getStringValue(severityObject);
	}

	private Severity getSeverityFromOtelLevel(int otelLevel) {
		if (otelLevel <= 4) {
			return Severity.TRACE;
		}
		if (otelLevel <= 8) {
			return Severity.DEBUG;
		}
		if (otelLevel <= 12) {
			return Severity.INFO;
		}
		if (otelLevel <= 16) {
			return Severity.WARN;
		}
		if (otelLevel <= 20) {
			return Severity.ERROR;
		}
		return Severity.FATAL;
	}

	private Instant toInstant(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Instant) {
			return (Instant) value;
		}
		if (value instanceof Date) {
			return ((Date) value).toInstant();
		}
		if (value instanceof Number) {
			return Instant.ofEpochMilli(((Number) value).longValue());
		}
		try {
			return Instant.ofEpochMilli(Long.parseLong(value.toString()));
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

	private String getStringValue(Object value) {
		return value == null ? null : value.toString();
	}

	private void putAttribute(AttributesBuilder builder, String key, Object value) {
		if (value == null) {
			return;
		}
		if (value instanceof Boolean) {
			builder.put(AttributeKey.booleanKey(key), ((Boolean) value).booleanValue());
			return;
		}
		if (value instanceof Integer
				|| value instanceof Long
				|| value instanceof Short
				|| value instanceof Byte) {
			builder.put(AttributeKey.longKey(key), ((Number) value).longValue());
			return;
		}
		if (value instanceof Float || value instanceof Double) {
			builder.put(AttributeKey.doubleKey(key), ((Number) value).doubleValue());
			return;
		}
		if (value instanceof Number) {
			builder.put(AttributeKey.doubleKey(key), ((Number) value).doubleValue());
			return;
		}
		builder.put(AttributeKey.stringKey(key), value.toString());
	}
}
