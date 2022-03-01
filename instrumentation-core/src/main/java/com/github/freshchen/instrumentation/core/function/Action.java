package com.github.freshchen.instrumentation.core.function;

/**
 * @author freshchen
 * @since 2022/1/25
 */
@FunctionalInterface
public interface Action {

    /**
     * no return
     */
    void execute();
}
