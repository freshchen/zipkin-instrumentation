package com.github.freshchen.vault.instrumentation.plan2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.CurrentTraceContext;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.http.HttpClientHandler;
import org.springframework.cloud.sleuth.http.HttpClientRequest;
import org.springframework.cloud.sleuth.http.HttpClientResponse;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.lang.Nullable;
import org.springframework.web.client.HttpStatusCodeException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static com.github.freshchen.vault.instrumentation.plan2.VaultTraceConfiguration2.IS_VAULT_SPAN;

/**
 * @author darcy
 * @since 2022/03/13
 **/
public class CustomTracingVaultClientHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    private static final Log log = LogFactory.getLog(CustomTracingVaultClientHttpRequestInterceptor.class);

    public static ClientHttpRequestInterceptor create(CurrentTraceContext currentTraceContext,
                                                      HttpClientHandler httpClientHandler) {
        return new CustomTracingVaultClientHttpRequestInterceptor(currentTraceContext, httpClientHandler);
    }

    final CurrentTraceContext currentTraceContext;

    final HttpClientHandler handler;

    @Autowired
    CustomTracingVaultClientHttpRequestInterceptor(CurrentTraceContext currentTraceContext,
                                                   HttpClientHandler httpClientHandler) {
        this.currentTraceContext = currentTraceContext;
        this.handler = httpClientHandler;
    }

    @Override
    public ClientHttpResponse
    intercept(HttpRequest req, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        CustomTracingVaultClientHttpRequestInterceptor.HttpRequestWrapper request =
                new CustomTracingVaultClientHttpRequestInterceptor.HttpRequestWrapper(req);
        Span span = handler.handleSend(request);
        // 新增标签
        span.tag(IS_VAULT_SPAN, Boolean.TRUE.toString());
        if (log.isDebugEnabled()) {
            log.debug("Wrapping an outbound http call with span [" + span + "]");
        }
        ClientHttpResponse response = null;
        Throwable error = null;
        try (CurrentTraceContext.Scope ws = currentTraceContext.newScope(span.context())) {
            response = execution.execute(req, body);
            return response;
        } catch (Throwable e) {
            error = e;
            throw e;
        } finally {
            handler.handleReceive(new CustomTracingVaultClientHttpRequestInterceptor
                    .ClientHttpResponseWrapper(request, response, error), span);
        }
    }

    static final class HttpRequestWrapper implements HttpClientRequest {

        final HttpRequest delegate;

        HttpRequestWrapper(HttpRequest delegate) {
            this.delegate = delegate;
        }

        @Override
        public Collection<String> headerNames() {
            return this.delegate.getHeaders().keySet();
        }

        @Override
        public Object unwrap() {
            return delegate;
        }

        @Override
        public String method() {
            return delegate.getMethod().name();
        }

        @Override
        public String path() {
            return delegate.getURI().getPath();
        }

        @Override
        public String url() {
            return delegate.getURI().toString();
        }

        @Override
        public String header(String name) {
            Object result = delegate.getHeaders().getFirst(name);
            return result != null ? result.toString() : null;
        }

        @Override
        public void header(String name, String value) {
            delegate.getHeaders().set(name, value);
        }

    }

    static final class ClientHttpResponseWrapper implements HttpClientResponse {

        final CustomTracingVaultClientHttpRequestInterceptor.HttpRequestWrapper request;

        @Nullable
        final ClientHttpResponse response;

        @Nullable
        final Throwable error;

        ClientHttpResponseWrapper(CustomTracingVaultClientHttpRequestInterceptor.HttpRequestWrapper request,
                                  @Nullable ClientHttpResponse response, @Nullable Throwable error) {
            this.request = request;
            this.response = response;
            this.error = error;
        }

        @Override
        public Object unwrap() {
            return response;
        }

        @Override
        public Collection<String> headerNames() {
            return this.response != null ? this.response.getHeaders().keySet() : Collections.emptyList();
        }

        @Override
        public CustomTracingVaultClientHttpRequestInterceptor.HttpRequestWrapper request() {
            return request;
        }

        @Override
        public Throwable error() {
            return error;
        }

        @Override
        public int statusCode() {
            try {
                int result = response != null ? response.getRawStatusCode() : 0;
                if (result <= 0 && error instanceof HttpStatusCodeException) {
                    result = ((HttpStatusCodeException) error).getRawStatusCode();
                }
                return result;
            } catch (Exception e) {
                return 0;
            }
        }

    }

}
