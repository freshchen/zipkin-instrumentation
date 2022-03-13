package com.github.freshchen.vault.instrumentation.plan2;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;

import static com.github.freshchen.vault.instrumentation.plan2.VaultTraceConfiguration2.IS_VAULT_SPAN;

/**
 * @author darcy
 * @since 2022/03/13
 **/
public class CustomTracingVaultSpanHandler extends SpanHandler {

    @Override
    public boolean end(TraceContext context, MutableSpan span, Cause cause) {
        if (Boolean.TRUE.toString().equals(span.tag(IS_VAULT_SPAN))) {
            span.localServiceName("vault");
            span.name(span.name() + " " + span.tag("http.path"));
            span.removeTag(IS_VAULT_SPAN);
        }
        return true;
    }
}
