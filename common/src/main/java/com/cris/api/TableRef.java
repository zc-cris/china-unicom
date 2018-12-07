package com.cris.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/*注解可以使用的范围*/
@Target({ElementType.TYPE})
/*运行时可以使用注解*/
@Retention(RetentionPolicy.RUNTIME)
public @interface TableRef {
    String value();
}
