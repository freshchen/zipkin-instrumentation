package com.github.freshchen.vault.instrumentation.plan1;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.sleuth.instrument.web.client.LazyTraceClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.vault.client.RestTemplateCustomizer;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;

/**
 * @author darcy
 * @since 2022/03/13
 **/
public class TracingVaultTemplateCustomizer implements RestTemplateCustomizer {

    private final BeanFactory beanFactory;

    public TracingVaultTemplateCustomizer(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * @param restTemplate
     * @see org.springframework.cloud.vault.config.VaultConfiguration#createRestTemplateBuilder
     */
    @Override
    public void customize(RestTemplate restTemplate) {
        ArrayList<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        // 如果 spring cloud 默认不会包装，因此不用检查是否已经包装过
        interceptors.add(0, new LazyTraceClientHttpRequestInterceptor(beanFactory));
        restTemplate.setInterceptors(interceptors);
    }
}
