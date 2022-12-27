package cn.schoolwow.quickdao.domain.internal;

import cn.schoolwow.quickdao.exception.SQLRuntimeException;

import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<T> extends Consumer<T> {
    @Override
    default void accept(final T elem) {
        try {
            acceptThrows(elem);
        } catch (Exception e) {
            throw new SQLRuntimeException(e);
        }
    }

    void acceptThrows(T elem) throws Exception;
}
