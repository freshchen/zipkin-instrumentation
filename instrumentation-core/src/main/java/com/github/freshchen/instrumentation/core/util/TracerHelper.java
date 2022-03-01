package com.github.freshchen.instrumentation.core.util;

import brave.Span;
import brave.Tracer;
import com.github.freshchen.instrumentation.core.function.Action;
import com.github.freshchen.instrumentation.core.function.ExceptionAction;
import com.github.freshchen.instrumentation.core.function.ExceptionSupplier;

import java.util.function.Supplier;

/**
 * @author freshchen
 * @since 2022/2/27
 */
public class TracerHelper {

    protected final Tracer tracer;

    public TracerHelper(Tracer tracer) {
        this.tracer = tracer;
    }

    public Span startNextSpan() {
        Span span = tracer.nextSpan();
        return span.start();
    }

    public <T> T executeInScope(Span span, Supplier<T> supplier) {
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return supplier.get();
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public void executeInScope(Span span, Action action) {
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            action.execute();
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public <T extends Exception> void executeInScopeThrowing(Span span, ExceptionAction<T> exceptionAction) throws T {
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            exceptionAction.execute();
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    public <T extends Exception, V> V executeInScopeThrowing(Span span, ExceptionSupplier<T, V> supplier) throws T {
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return supplier.get();
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

}