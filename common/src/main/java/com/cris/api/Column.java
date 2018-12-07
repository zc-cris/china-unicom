package com.cris.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * 列族名字
     **/
    String columnFamily() default "info";

    /**
     * 列名字
     **/
    String column() default "";
}
