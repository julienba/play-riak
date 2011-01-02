package play.modules.riak;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONException;

import play.Logger;
import play.Play;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.mvc.Http;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.mapreduce.JavascriptFunction;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.MapReduceResponse;
import com.basho.riak.client.response.StoreResponse;
import com.google.gson.Gson;

//http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
public class RiakModel {
	
	// Raw object
	private RiakObject obj;
	
	public RiakObject getObj(){
		return obj;
	}
	public void setObj(RiakObject obj){
		this.obj = obj;
	}
	
	
	public Map<String,String> getUserMeta(){
		return obj.getUsermeta();
	}
	
	public void setUserMeta(Map<String,String> usermeta){
		obj.setUsermeta(usermeta);
	}	
	
	public static String generateUID(){
		return  String.valueOf(UUID.randomUUID());
	}

	public String toJSON(){
		RiakObject o = null;
		// Hack to prevent serialisation of obj
		if(this.obj != null){
			o = this.obj;
		}
		
		String jsonValue = new Gson().toJson(this);
		
		this.obj = o;
		return jsonValue;
	}
	
	public boolean save() {
		Logger.debug("RiakModel save %s", this.toString());
		
		RiakObject o = null;
		
		// Hack to prevent serialisation of obj
		if(this.obj != null){
			o = this.obj;
		}
		this.obj = null;
		
		String jsonValue = new Gson().toJson(this);
		RiakPath path = this.getPath();
		
		if(path != null){
			
			if(o == null){
				Logger.debug("Create new object %s, %s", path.getBucket(), path.getValue());
				o = new RiakObject(path.getBucket(), path.getValue());	
			}
			
			o.setValue(jsonValue);
			Logger.debug("Store %d links", o.getLinks().size());
			StoreResponse storeResponse = RiakPlugin.riak.store(o);
			
			if(storeResponse.isError() == true || storeResponse.isSuccess() == false){
				Logger.error("Error during save: %s", storeResponse.getBodyAsString());
				return false;
			}	
			this.obj = o;
			
			return true;
			
		}else{
			return false;
		}
	}

	public RiakPath getPath(){
		Class clazz = this.getClass();
		
		RiakKey key = RiakPlugin.bucketMap.get(clazz.getName());
		try {
			Field f = clazz.getField(key.getKey());
			String value = f.get(this).toString();
			RiakPath path = new RiakPath(value, key);
			return path;
		} catch (Exception e) {
			Logger.error("Error %s during reflextion of class: %s with path: %s", e.getMessage(), clazz.getName(), key.toString());
			e.printStackTrace();
			return null;
		}
	}
	
	public static <T extends RiakModel> List<T> findAll(Class clazz){
		throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
	}
	public static <T extends RiakModel> List<T> findAll(String bucket){
		throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
	}
	
	public static List findOrderByDate(Class clazz, Type returnType, boolean reverse){
		try {
			MapReduceResponse r = null;
			
			if(reverse){
				r = RiakPlugin.riak.mapReduceOverBucket(RiakPlugin.getBucketName(clazz))
					.map(JavascriptFunction.anon(RiakMP.orderByCreationDateMapString), false)
					.reduce(JavascriptFunction.anon(RiakMP.orderByCreateDateDescReduceString), false)
					.reduce(JavascriptFunction.anon(RiakMP.cleanReduce), true).submit();
			}else{
				r = RiakPlugin.riak.mapReduceOverBucket(RiakPlugin.getBucketName(clazz))
					.map(JavascriptFunction.anon(RiakMP.orderByCreationDateMapString), false)
					.reduce(JavascriptFunction.anon(RiakMP.orderByCreateDateAscReduceString), false)
					.reduce(JavascriptFunction.anon(RiakMP.cleanReduce), true).submit();
			}
		    if (r.isSuccess()) {
		    	List jsonResult = new Gson().fromJson(r.getBodyAsString(), returnType);
		    	return jsonResult;
		    }else
		    	return null;
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}		
	}
	public static List findOrderByDate(Class clazz, Type returnType){
		return findOrderByDate(clazz, returnType, false);
	}
	
	public static <T extends RiakModel> List<T> fetch(Class clazz, Type returnType, int start, int end){
		throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
	}
	
	public static Collection<String> findKeys(Class clazz){
		return findKeys(RiakPlugin.getBucketName(clazz)); 
	}
	public static Collection<String> findKeys(String bucket){
		
		BucketResponse r = RiakPlugin.riak.listBucket(bucket);
		return r.getBucketInfo().getKeys();		
	}
	
	public static <T extends RiakModel> T find(Class clazz, String key){
		throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
	}
	public static <T extends RiakModel> T find(String bucket, String key){
		throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
	}	
	
	public static boolean delete(Class clazz, String key){
		return delete(RiakPlugin.getBucketName(clazz), key);
	}
	public static boolean delete(String bucket, String key){
		HttpResponse response = RiakPlugin.riak.delete(bucket, key);
		if(response.getStatusCode() == Http.StatusCode.NO_RESPONSE)
			return true;
		else{
			Logger.error("Error during delete of bucket: %s, key: %s with message: %s", bucket,key,response.getBodyAsString());
			return false;
		}		
	}
	
	public boolean delete(){
		RiakPath path = this.getPath();
		if(path != null){
			Logger.debug("Delete bucket: %s , keyValue %s", path.getBucket(), path.getValue());
			return delete(path.getBucket(), path.getValue());
		}		
		return false;
	}
	public static void deleteAll(Class clazz){
		deleteAll(RiakPlugin.getBucketName(clazz));
	}
	public static void deleteAll(String bucket){
		
		Collection<String> keys = RiakModel.findKeys(bucket);
		
		for(String key: keys){
			RiakModel.delete(bucket, key);
		}
	}
	
	public List<RiakModel> getLinksByTag(String tag){

		List<RiakModel> result = new ArrayList<RiakModel>();
		
		List<RiakLink> links = obj.getLinks();
		Set<String> keys = RiakPlugin.bucketMap.keySet();
		
		for (RiakLink link : links) {
			
			if(link.getTag().equals(tag)){
				String bucket  = link.getBucket();	
				
				for(String key: keys){
					RiakKey riakKey = RiakPlugin.bucketMap.get(key);
					if(riakKey.getBucket().equals(bucket)){
						
						try{
							ApplicationClass clazz = Play.classes.getApplicationClass(key);
							Method m = clazz.javaClass.getMethod("find", new Class[]{String.class, String.class});
							RiakModel r = (RiakModel)m.invoke(new Object(), bucket, link.getKey());						
							result.add(r);
						} catch (Exception e) {
							Logger.error("Errror during reflection");
							e.printStackTrace();
						}		
						
						break;					
					}
				}
			}
		}		
		
		return result;		
		
	}
	
	public List<RiakModel> getLink(){
		List<RiakModel> result = new ArrayList<RiakModel>();
		
		List<RiakLink> links = obj.getLinks();
		Set<String> keys = RiakPlugin.bucketMap.keySet();
		
		for (RiakLink link : links) {
			String bucket  = link.getBucket();	
			
			for(String key: keys){
				RiakKey riakKey = RiakPlugin.bucketMap.get(key);
				if(riakKey.getBucket().equals(bucket)){
					
					try{
						ApplicationClass clazz = Play.classes.getApplicationClass(key);
						Method m = clazz.javaClass.getMethod("find", new Class[]{String.class, String.class});
						RiakModel r = (RiakModel)m.invoke(new Object(), bucket, link.getKey());						
						result.add(r);
					} catch (Exception e) {
						Logger.error("Errror during reflection");
						e.printStackTrace();
					}		
					
					break;					
				}
			}
		}		
		
		return result;
	}
	
	public List<RiakLink> getRawLink(){
		if(obj != null)
			return obj.getLinks();
		else
			return new ArrayList<RiakLink>();
	}
	
	public List<RiakLink> getRawLinksByTag(String tag){
		List<RiakLink> res = new ArrayList<RiakLink>(); 
		if(this.obj !=null){
			List<RiakLink> links = obj.getLinks();
			for (RiakLink link : links) {
				if(link.getTag().equals(tag)){
					res.add(link);
				}
			}
		}
		
		return res;		
	}

	public void addLink(Class clazz, String key, String tag){
		this.addLink(RiakPlugin.getBucketName(clazz), key, tag);
	}	
	public void addLink(String bucket, String key, String tag){
		obj.addLink(new RiakLink(bucket, key, tag));
	}
	
	public static <T extends RiakModel> List<T> jsonToList(String json, Type listType){
		throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
	}
	
	public static List orderBy(Class clazz, String field, Boolean reverse , Type listType){
		Object[] args = {field, reverse};
		try {
			MapReduceResponse r = play.modules.riak.RiakPlugin.riak.mapReduceOverBucket(play.modules.riak.RiakPlugin.getBucketName(clazz))
				.map(com.basho.riak.client.mapreduce.JavascriptFunction.named("Riak.mapValuesJson"), false)
				.reduce(com.basho.riak.client.mapreduce.JavascriptFunction.anon(play.modules.riak.RiakMP.orderByReduceString), args, true).submit();
			if (r.isSuccess()) {
				return new com.google.gson.Gson().fromJson(r.getBodyAsString(), listType);
			}
			else{
				return null;
			}
		} catch (org.json.JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
}
