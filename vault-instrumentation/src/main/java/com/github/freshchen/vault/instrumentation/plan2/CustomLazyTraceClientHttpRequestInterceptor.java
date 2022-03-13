package com.github.freshchen.vault.instrumentation.plan2;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.sleuth.internal.ContextUtil;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * @author darcy
 * @since 2022/03/13
 **/
public class CustomLazyTraceClientHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    private final BeanFactory beanFactory;

    private CustomTracingVaultClientHttpRequestInterceptor interceptor;

    public CustomLazyTraceClientHttpRequestInterceptor(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        if (isContextUnusable()) {
            return execution.execute(request, body);
        }
        return interceptor().intercept(request, body, execution);
    }

    boolean isContextUnusable() {
        return ContextUtil.isContextUnusable(this.beanFactory);
    }

    ClientHttpRequestInterceptor interceptor() {
        if (this.interceptor == null) {
            this.interceptor = this.beanFactory.getBean(CustomTracingVaultClientHttpRequestInterceptor.class);
        }
        return this.interceptor;
    }

}
