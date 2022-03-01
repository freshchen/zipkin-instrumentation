package com.github.freshchen.instrumentation.core.function;

/**
 * @author freshchen
 * @since 2022/1/25
 */
@FunctionalInterface
public interface ExceptionAction<T extends Exception> {

    /**
     * may throw Exception
     * @throws T
     */
    void execute() throws T;
}
