package com.github.freshchen.vault.instrumentation.plan1;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.vault.client.RestTemplateCustomizer;

/**
 * @author darcy
 * @since 2022/03/13
 **/
@Configuration(proxyBeanMethods = false)
public class VaultTraceConfiguration1 {

    /**
     * 方案一
     *
     * @param beanFactory
     * @return
     */
    @Bean
    public RestTemplateCustomizer tracingRestTemplateCustomizer(BeanFactory beanFactory) {
        return new TracingVaultTemplateCustomizer(beanFactory);
    }
}
