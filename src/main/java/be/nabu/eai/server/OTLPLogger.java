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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.LoggerFactory;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.Element;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.ComplexEventImpl;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.SdkLoggerProviderBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OTLPLogger implements EventHandler<Object, Void> {
	private static final Duration OTEL_TIMEOUT = Duration.ofSeconds(15);
	private static final String INSTRUMENTATION_NAME = "be.nabu.eai.events";
	private static final String OTEL_SERVICE_NAME = "nabu";
	private final EventSeverity otlpLogSeverity = EventSeverity.valueOf(System.getProperty("otlp.log_severity", "INFO"));
	private final EventSeverity otlpSpanSeverity = EventSeverity.valueOf(System.getProperty("otlp.span_severity", "DEBUG"));
	private final boolean debug = Boolean.parseBoolean(System.getProperty("otlp.debug", "false"));
	private final boolean OTEL_LOGS_AS_SPANS = Boolean.parseBoolean(System.getProperty("otlp.logs_as_spans", "true"));
	private final boolean OTEL_SPANS_AS_LOGS = Boolean.parseBoolean(System.getProperty("otlp.spans_as_logs", "true"));
	private static final Map<String, String> MAPPINGS = Map.ofEntries(
			// this overwrites the service which should be static. instead set as resource.name (see below)
//		Map.entry("artifactId", "service.name"),
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
	private final SdkLoggerProvider loggerProvider;
	private final io.opentelemetry.api.logs.Logger otelLogger;
	private final SpanExporter spanExporter;
	private final LogRecordExporter logExporter;
	private final Resource resource;
	private final InstrumentationScopeInfo instrumentationInfo;

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

		this.resource = Resource.getDefault()
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

		SdkLoggerProviderBuilder loggerBuilder = SdkLoggerProvider.builder()
			.setResource(this.resource);
		if (logExporter != null) {
			loggerBuilder.addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build());
		}
		this.loggerProvider = loggerBuilder.build();
		this.spanExporter = spanExporter;
		this.logExporter = logExporter;
		this.instrumentationInfo = InstrumentationScopeInfo.create(INSTRUMENTATION_NAME);

		if (spanExporter != null && logExporter != null) {
			logger.info("OTLP logger initialized using {} at {}", protocol, otelEndpoint);
		}
		else {
			logger.warn("OTLP logger initialized without exporters using {} at {}", protocol, otelEndpoint);
		}

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
			if (debug) {
				logger.debug("OTLPLogger skipped event: unable to wrap {}", event);
			}
			return null;
		}
		
		EventSeverity severity = getEventSeverity(wrapped.get("severity"));

		boolean logAsSpan = OTEL_LOGS_AS_SPANS && (severity == null || severity.ordinal() >= otlpSpanSeverity.ordinal());
		boolean logAsLog = (!logAsSpan || OTEL_SPANS_AS_LOGS) && (severity == null || severity.ordinal() >= otlpLogSeverity.ordinal());
		
		Attributes attributes = buildAttributes(wrapped);
		Instant started = toInstant(wrapped.get("started"));
		Instant stopped = toInstant(wrapped.get("stopped"));
		String traceId = getTraceId(wrapped);
		String spanId = getSpanId(wrapped);
		String parentSpanId = getParentSpanId(wrapped);

		if (logAsSpan && started != null && stopped != null && traceId != null && spanId != null) {
			String spanName = getSpanName(wrapped);
			SpanData spanData = createSpanData(traceId, spanId, parentSpanId, spanName, attributes, severity, started, stopped);
			exportSpan(spanData);
			if (debug) {
				logger.debug("OTLPLogger emitted span name={} traceId={} spanId={} parentSpanId={}", spanName, traceId, spanId, parentSpanId);
			}
		}
		// either we are not sending logs as spans or we want all spans to be duplicated to the logs
		if (logAsLog) {
			if (debug) {
				logger.debug("OTLPLogger falling back to log started={} stopped={} traceId={} spanId={}", started, stopped, traceId, spanId);
			}
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

			Context logContext = getLogContext(traceId, spanId, parentSpanId);
			if (logContext != null) {
				logBuilder.setContext(logContext);
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
		if (spanExporter != null) {
			try {
				spanExporter.shutdown();
			}
			catch (Exception e) {
				logger.warn("Failed to shutdown OTLP span exporter", e);
			}
		}
		if (logExporter != null) {
			try {
				logExporter.shutdown();
			}
			catch (Exception e) {
				logger.warn("Failed to shutdown OTLP log exporter", e);
			}
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

		for (Element<?> element : wrapped.getType()) {
			String name = element.getName();
			if (name == null || MAPPINGS.containsKey(name)) {
				continue;
			}
			Object value = wrapped.get(name);
			if (value == null) {
				continue;
			}
			putAttribute(builder, name, value);
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

		EventSeverity severity = getEventSeverity(wrapped.get("severity"));
		if (severity != null) {
			builder.put(AttributeKey.longKey("severity_number"), severity.getOtelLevel());
			builder.put(AttributeKey.stringKey("severity_text"), severity.name());
		}
		String resourceName = getStringValue(wrapped.get("artifactId"));
		if (resourceName == null) {
			resourceName = getStringValue(wrapped.get("eventName"));
		}
		if (resourceName != null) {
			builder.put(AttributeKey.stringKey("resource.name"), resourceName);
		}
		String traceId = getTraceId(wrapped);
		if (traceId != null) {
			builder.put(AttributeKey.stringKey("trace_id"), traceId);
		}
		String spanId = getSpanId(wrapped);
		if (spanId != null) {
			builder.put(AttributeKey.stringKey("span_id"), spanId);
		}
		String parentSpanId = getParentSpanId(wrapped);
		if (parentSpanId != null) {
			builder.put(AttributeKey.stringKey("parent_span_id"), parentSpanId);
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

	private Context getLogContext(String traceId, String spanId, String parentSpanId) {
		if (traceId == null) {
			return null;
		}
		String contextSpanId = spanId != null ? spanId : parentSpanId;
		if (contextSpanId == null) {
			return null;
		}
		if (!TraceId.isValid(traceId) || !SpanId.isValid(contextSpanId)) {
			logger.debug("Invalid trace/span id for log context: traceId={}, spanId={}", traceId, contextSpanId);
			return null;
		}
		SpanContext spanContext = SpanContext.createFromRemoteParent(traceId, contextSpanId, TraceFlags.getSampled(), TraceState.getDefault());
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

	private String getSpanId(ComplexContent wrapped) {
		String spanId = getStringValue(wrapped.get("spanId"));
		if (spanId != null && SpanId.isValid(spanId)) {
			return spanId;
		}
		return null;
	}

	private String getParentSpanId(ComplexContent wrapped) {
		String parentSpanId = getStringValue(wrapped.get("parentSpanId"));
		if (parentSpanId != null && SpanId.isValid(parentSpanId)) {
			return parentSpanId;
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

	private StatusData getStatusData(EventSeverity severity) {
		if (severity != null && severity.getOtelLevel() >= 17) {
			return StatusData.create(io.opentelemetry.api.trace.StatusCode.ERROR, severity.name());
		}
		return StatusData.unset();
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

	private SpanData createSpanData(String traceId, String spanId, String parentSpanId, String spanName,
			Attributes attributes, EventSeverity severity, Instant started, Instant stopped) {
		SpanContext spanContext = SpanContext.create(traceId, spanId, TraceFlags.getSampled(), TraceState.getDefault());
		SpanContext parentSpanContext = parentSpanId == null
			? SpanContext.getInvalid()
			: SpanContext.createFromRemoteParent(traceId, parentSpanId, TraceFlags.getSampled(), TraceState.getDefault());
		long startEpochNanos = toEpochNanos(started);
		long endEpochNanos = toEpochNanos(stopped);
		StatusData statusData = getStatusData(severity);
		return new OTLPSpanData(spanName, SpanKind.INTERNAL, spanContext, parentSpanContext, statusData,
				startEpochNanos, endEpochNanos, attributes, instrumentationInfo, resource);
	}

	private void exportSpan(SpanData spanData) {
		if (spanExporter == null) {
			logger.debug("OTLP span exporter not available; dropping span {}", spanData.getName());
			return;
		}
		CompletableResultCode result = spanExporter.export(Collections.singletonList(spanData));
		result.whenComplete(() -> {
			if (!result.isSuccess()) {
				logger.warn("Failed to export OTLP span {}", spanData.getName(), result.getFailureThrowable());
			}
		});
	}

	private long toEpochNanos(Instant instant) {
		return instant.getEpochSecond() * 1000000000L + instant.getNano();
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
		if (value instanceof TimeZone) {
			TimeZone timeZone = (TimeZone) value;
			String zoneId = timeZone.getID();
			builder.put(AttributeKey.stringKey(key), zoneId != null ? zoneId : timeZone.toString());
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

	private static final class OTLPSpanData implements SpanData {
		private final String name;
		private final SpanKind kind;
		private final SpanContext spanContext;
		private final SpanContext parentSpanContext;
		private final StatusData status;
		private final long startEpochNanos;
		private final long endEpochNanos;
		private final Attributes attributes;
		private final List<EventData> events;
		private final List<LinkData> links;
		private final InstrumentationScopeInfo instrumentationScopeInfo;
		private final Resource resource;

		private OTLPSpanData(String name, SpanKind kind, SpanContext spanContext, SpanContext parentSpanContext,
				StatusData status, long startEpochNanos, long endEpochNanos, Attributes attributes,
				InstrumentationScopeInfo instrumentationScopeInfo, Resource resource) {
			this.name = name;
			this.kind = kind;
			this.spanContext = spanContext;
			this.parentSpanContext = parentSpanContext;
			this.status = status;
			this.startEpochNanos = startEpochNanos;
			this.endEpochNanos = endEpochNanos;
			this.attributes = attributes;
			this.events = Collections.emptyList();
			this.links = Collections.emptyList();
			this.instrumentationScopeInfo = instrumentationScopeInfo;
			this.resource = resource;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public SpanKind getKind() {
			return kind;
		}

		@Override
		public SpanContext getSpanContext() {
			return spanContext;
		}

		@Override
		public SpanContext getParentSpanContext() {
			return parentSpanContext;
		}

		@Override
		public StatusData getStatus() {
			return status;
		}

		@Override
		public long getStartEpochNanos() {
			return startEpochNanos;
		}

		@Override
		public Attributes getAttributes() {
			return attributes;
		}

		@Override
		public List<EventData> getEvents() {
			return events;
		}

		@Override
		public List<LinkData> getLinks() {
			return links;
		}

		@Override
		public long getEndEpochNanos() {
			return endEpochNanos;
		}

		@Override
		public boolean hasEnded() {
			return true;
		}

		@Override
		public int getTotalRecordedEvents() {
			return events.size();
		}

		@Override
		public int getTotalRecordedLinks() {
			return links.size();
		}

		@Override
		public int getTotalAttributeCount() {
			return attributes.size();
		}

		@Override
		public InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
			return InstrumentationLibraryInfo.empty();
		}

		@Override
		public InstrumentationScopeInfo getInstrumentationScopeInfo() {
			return instrumentationScopeInfo;
		}

		@Override
		public Resource getResource() {
			return resource;
		}
	}
}
