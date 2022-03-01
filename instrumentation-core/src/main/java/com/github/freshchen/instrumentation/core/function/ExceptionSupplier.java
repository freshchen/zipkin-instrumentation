package com.github.freshchen.instrumentation.core.function;

/**
 * @author freshchen
 * @since 2022/2/7
 */
@FunctionalInterface
public interface ExceptionSupplier<T extends Exception, V> {

    /**
     * may throw Exception
     * @throws T
     */
    V get() throws T;
}