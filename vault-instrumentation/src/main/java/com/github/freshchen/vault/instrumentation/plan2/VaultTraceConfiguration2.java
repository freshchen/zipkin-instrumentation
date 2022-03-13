package com.github.freshchen.vault.instrumentation.plan2;

import brave.handler.SpanHandler;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.sleuth.CurrentTraceContext;
import org.springframework.cloud.sleuth.http.HttpClientHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.vault.client.RestTemplateCustomizer;

/**
 * @author darcy
 * @since 2022/03/13
 **/
@Configuration(proxyBeanMethods = false)
public class VaultTraceConfiguration2 {

    public static final String IS_VAULT_SPAN = "isVaultSpan";

    /**
     * 方案二
     *
     * @param beanFactory
     * @return
     */
    @Bean
    public RestTemplateCustomizer tracingRestTemplateCustomizer(BeanFactory beanFactory) {
        return new CustomTracingVaultTemplateCustomizer(beanFactory);
    }

    @Bean
    public CustomTracingVaultClientHttpRequestInterceptor
    tracingRestTemplateCustomizer(CurrentTraceContext currentTraceContext,
                                  HttpClientHandler httpClientHandler) {
        return new CustomTracingVaultClientHttpRequestInterceptor(currentTraceContext, httpClientHandler);
    }

    @Bean
    public SpanHandler customTracingVaultSpanHandler() {
        return new CustomTracingVaultSpanHandler();
    }


}
