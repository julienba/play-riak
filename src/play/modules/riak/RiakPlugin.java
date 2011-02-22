package play.modules.riak;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.classloading.ApplicationClasses.ApplicationClass;

import com.basho.riak.pbc.RiakClient;

public class RiakPlugin extends PlayPlugin{
	
	/**
	 * riak url like http://localhost:8091/riak
	 */
	public static String RIAK_URL 	= "";
	public static int	 RIAK_PORT 	= 0;
	/**
	 * riak url without /riak
	 */
	public static String RIAK_BASE 	= "";
	/**
	 * Hardcode URL in riak
	 */
	public static String RIAK_URL_STATS = "stats";
	
	/**
	 * main client
	 */
	public static RiakClient riak = null;
	
	public static String DEFAULT_MODEL_PREFIX = "models.riak.";
	public static String MODEL_PREFIX;
	
	private RiakEnhancer enhancer = new RiakEnhancer();

	public static Map<String,RiakKey> bucketMap = new HashMap<String,RiakKey>();
	
	public static String getBucketName(Class clazz){
		String className = clazz.getName();
		RiakKey key = bucketMap.get(className);
		if(key == null){
			Logger.error("No bucket define for className %s", className);
			return "";
		}
		return key.getBucket();
	}
	
	@Override
	public void enhance(ApplicationClass applicationClass) throws Exception {
		enhancer.enhanceThisClass(applicationClass);
	}
	
	public boolean init(){
		
		RIAK_URL = Play.configuration.getProperty("riak.protobuf.url");
		RIAK_PORT = Integer.valueOf(Play.configuration.getProperty("riak.protobuf.port"));
		if(RIAK_URL.isEmpty() ||RIAK_PORT == 0){
			Logger.error("riak.protobuf.url or riak.protobuf.port is empty");
			return false;
		}		
		
		try {
			Logger.info("Init riak client (url: %s )", RIAK_URL);
			riak = new RiakClient(RIAK_URL);
			riak.ping();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	@Override
    public void onApplicationStart() {
		boolean res = init();
		if(res == false)
			Logger.error("Riak %s cluster not responding !", RIAK_URL);
		else{
			// Retrieve key and bucket for all models
			RiakPlugin.bucketMap = new HashMap<String,RiakKey>();
			
			List<ApplicationClass> classes = Play.classes.getAnnotatedClasses(RiakEntity.class);
			for (ApplicationClass clazz : classes) {			
				RiakEntity annotation = (RiakEntity) clazz.javaClass.getAnnotation(RiakEntity.class);
				
				String key = annotation.key();
				String entityName = clazz.javaClass.getName();
				
				if(key.equals("")){
					Logger.error("Key for class %s is not defined", this.toString());
				}else{
					String bucket = annotation.bucket();
					if(bucket.equals("")){
						bucket = entityName;
						bucket = bucket.substring(bucket.lastIndexOf(".") + 1);
					}
					
					RiakPlugin.bucketMap.put(entityName, new RiakKey(bucket, key));
				}				
			}
		}
    }
}
