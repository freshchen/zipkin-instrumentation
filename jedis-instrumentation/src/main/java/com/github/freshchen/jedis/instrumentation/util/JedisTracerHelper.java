package com.github.freshchen.jedis.instrumentation.util;

import brave.Span;
import brave.Tracer;
import com.github.freshchen.instrumentation.core.util.TracerHelper;

import java.util.Arrays;
import java.util.Objects;

import static com.github.freshchen.jedis.instrumentation.util.JedisConstants.KEY;


/**
 * @author freshchen
 * @since 2022/3/1
 */
public class JedisTracerHelper extends TracerHelper {


    public JedisTracerHelper(Tracer tracer) {
        super(tracer);
    }

    public Span startNextJedisSpan(String command) {
        Span span = startNextSpan();
        span.kind(Span.Kind.CLIENT);
        span.name(command);
        span.remoteServiceName(JedisConstants.REDIS);
        return span;
    }

    public Span startNextJedisSpan(String command, Object key) {
        Span span = startNextJedisSpan(command);
        span.tag(KEY, Objects.toString(key));
        return span;
    }

    public Span startNextJedisSpan(String command, byte[] key) {
        Span span = startNextJedisSpan(command);
        span.tag(KEY, Arrays.toString(key));
        return span;
    }

    public Span startNextJedisSpan(String command, Object[] keys) {
        Span span = startNextJedisSpan(command);
        span.tag(KEY, Arrays.toString(keys));
        return span;
    }

}