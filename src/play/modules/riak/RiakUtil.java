package play.modules.riak;

import java.util.Set;

public class RiakUtil {

	public static void deleteAllDefineBucket(){
		Set<String> keys = RiakPlugin.bucketMap.keySet();
		for (String key : keys) {
			RiakModel.deleteAll(RiakPlugin.bucketMap.get(key).getBucket());
		}
	}
}
