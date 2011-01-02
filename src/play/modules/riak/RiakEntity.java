package play.modules.riak;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RiakEntity {

	String bucket() default "";
	String key() default "";
}

