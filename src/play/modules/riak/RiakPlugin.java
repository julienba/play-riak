package play.modules.riak;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.libs.WS;
import play.libs.WS.HttpResponse;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;

public class RiakPlugin extends PlayPlugin{
	
	/**
	 * riak url like http://localhost:8091/riak
	 */
	public static String RIAK_URL 	= "";
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
	/**
	 * Default property
	 */
	public static String RIAK_TIMEOUT = "2000";
	public static String RIAK_MAX_CONNECTION = "50";
	
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
		
		RIAK_URL = Play.configuration.getProperty("riak.url");
		RIAK_BASE = RIAK_URL.substring(0, RIAK_URL.lastIndexOf("/") + 1);
		RIAK_URL = Play.configuration.getProperty("riak.url");
		
		if(RIAK_URL.isEmpty()){
			Logger.error("riak.url is empty");
		}
		
		MODEL_PREFIX = Play.configuration.getProperty("riak.model.prefix", DEFAULT_MODEL_PREFIX);
		
		Logger.info("Init riak client (url: %s )", RIAK_URL);
		
		RiakConfig config = new RiakConfig(RIAK_URL);
		
		int timeout = Integer.parseInt(Play.configuration.getProperty("riak.timeout", RIAK_TIMEOUT));
		int maxConnection = Integer.parseInt(Play.configuration.getProperty("riak.maxConnection", RIAK_MAX_CONNECTION));
		
		config.setTimeout(timeout);
		config.setMaxConnections(maxConnection);
		riak = new RiakClient(config);
		
		// make a first request for retrieve base information and check that URL is good
		Logger.debug("Call stats url for check riak avaibility (url %s%s%s )",RIAK_URL,"/", RIAK_URL_STATS);
		HttpResponse response = WS.url(RIAK_URL + "/" + RIAK_URL_STATS).get();
		if(response.getStatus() == 200)	
			return true;
		else
			return false;
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
