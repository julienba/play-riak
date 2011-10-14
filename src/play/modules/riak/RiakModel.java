package play.modules.riak;

import static play.modules.riak.RiakPlugin.riak;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import play.Logger;
import play.Play;
import play.classloading.ApplicationClasses.ApplicationClass;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.MapReduce;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.google.gson.Gson;

public class RiakModel {
    
    // Raw object
    private IRiakObject obj;
    
    public IRiakObject getObj(){
        return obj;
    }
    public void setObj(IRiakObject obj){
        this.obj = obj;
    }
    
    public Iterable<Map.Entry<String,String>> getUserMeta(){
        return obj.userMetaEntries();
    }
    
    public void setUserMeta(Map<String,String> usermeta){
        for(Map.Entry<String, String> entry : usermeta.entrySet()) {
            obj.addUsermeta(entry.getKey(), entry.getValue());
        }
    }   
    
    public static String generateUID(){
        return  String.valueOf(UUID.randomUUID());
    }

    public String toJSON(){
        IRiakObject o = null;
        // Hack to prevent serialisation of obj
        if(this.obj != null){
            o = this.obj;
            this.obj = null;
        }
        
        String jsonValue = new Gson().toJson(this);
        this.obj = o;
        return jsonValue;
    }
    
    public boolean save() {
        Logger.debug("RiakModel save %s", this.toString());

        String jsonValue = toJSON();
        RiakPath path = this.getPath();
        
        if(path != null){
            Logger.debug("Create new object %s, %s", path.getBucket(), path.getValue());
            try {
            	Bucket bucket = RiakPlugin.riak.createBucket(path.getBucket()).execute();
            	IRiakObject riakObject = bucket.store(path.getValue(), jsonValue).returnBody(true).execute();
//            	if(this.obj != null && this.obj.getLinks() != null){
//            		for (RiakLink riakLink : this.obj.getLinks()) {
//						riakObject.addLink(riakLink);
//					}
//            	}
            	
            	this.setObj(riakObject);
            	
            	return true;
            } catch (RiakException e) {
            	e.printStackTrace();
            	return false;
            }
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
        String bucket = RiakPlugin.getBucketName(clazz);
        IRiakClient client = RiakPlugin.riak;

        MapReduce mr = client.mapReduce(bucket)
            .addMapPhase(new JSSourceFunction(RiakMapReduce.function.get("orderByCreationDateMap")), false);
        if(reverse)
            mr = mr.addReducePhase(new JSSourceFunction(RiakMapReduce.function.get("orderByCreateDateDescReduce")), false);
        else
            mr = mr.addReducePhase(new JSSourceFunction(RiakMapReduce.function.get("orderByCreateDateAscReduce")), false);
        
        mr = mr.addReducePhase(new JSSourceFunction(RiakMapReduce.function.get("cleanReduce")), true);
        try {
            MapReduceResult mrs = mr.execute();
            String res = mrs.getResultRaw();
            
            if(!res.isEmpty()){
                List jsonResult = new Gson().fromJson(res, returnType);
                return jsonResult;
            }
        }catch (RiakException e) {
            Logger.error("Error during findOrderByDate");
            e.printStackTrace();
        }   
    
        return null;
    }
    public static List findOrderByDate(Class clazz, Type returnType){
        return findOrderByDate(clazz, returnType, false);
    }
    
    //TODO: fix in enhancer
    public static <T extends RiakModel> List<T> fetch(Class clazz, Type returnType, int start, int end){
        throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
    }
    
    public static Iterable<String> findKeys(Class clazz){
        return findKeys(RiakPlugin.getBucketName(clazz)); 
    }
    public static Iterable<String> findKeys(String bucket){
        try {
            return riak.fetchBucket(bucket)
                .execute()
                .keys();
        } catch (RiakException e) {
            Logger.error("Error during listKeys for bucket: %s", bucket);
            e.printStackTrace();
            return null;
        }       
    }
    
    public static <T extends RiakModel> T find(Class clazz, String key){
        throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
    }
    public static <T extends RiakModel> T find(String bucket, String key){
        throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
    }   
    
    public static void delete(Class clazz, String key){
        delete(RiakPlugin.getBucketName(clazz), key);
    }
    public static void delete(String bucket, String key){
        try {
            riak.fetchBucket(bucket)
                .execute()
                .delete(key)
                .execute();
        } catch (RiakException e) {
            Logger.error("Error during deletion of bucket: %s, key: %s", bucket, key);
            e.printStackTrace();
        }   
    }
    
    public boolean delete(){
        RiakPath path = this.getPath();
        if(path != null){
            Logger.debug("Delete bucket: %s , keyValue %s", path.getBucket(), path.getValue());
            delete(path.getBucket(), path.getValue());
        }       
        return false;
    }
    public static void deleteAll(Class clazz){
        deleteAll(RiakPlugin.getBucketName(clazz));
    }
    public static void deleteAll(String bucket){
        Iterable<String> keys = RiakModel.findKeys(bucket);
        
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
        if(links ==null)
            return result;
        
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
        
        if(this.obj != null){
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

    	if(obj == null){
    		Logger.error("RiakObject [%s,%s] is null, save Entity and add link after", bucket, key);
    	}else{
    		obj.addLink(new RiakLink(tag, bucket, key));
    	}
    }
    
    public static <T extends RiakModel> List<T> jsonToList(String json, Type listType){
        throw new UnsupportedOperationException("Please annotate your model with @RiakEntity annotation.");
    }
    
    public static List orderBy(Class clazz, String field, Boolean reverse , Type listType){
        Object[] args = {field, reverse};
        String bucket = RiakPlugin.getBucketName(clazz);
        IRiakClient client = RiakPlugin.riak;

        MapReduce mr = client.mapReduce(bucket)
            .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"), false)
            .addReducePhase(new JSSourceFunction(RiakMapReduce.function.get("orderByReduce")), args, true);
        try {
            MapReduceResult mrs = mr.execute();
            String res = mrs.getResultRaw();
            return new com.google.gson.Gson().fromJson(res, listType);
        }catch (RiakException e) {
            e.printStackTrace();
        }               
        
        return null;
    }
    
//  public static List orderBy(Class clazz, String field, Boolean reverse , Type listType){
//      Object[] args = {field, reverse};
//      try {
//          MapReduceResponse r = play.modules.riak.RiakPlugin.riak.mapReduceOverBucket(play.modules.riak.RiakPlugin.getBucketName(clazz))
//              .map(com.basho.riak.client.mapreduce.JavascriptFunction.named("Riak.mapValuesJson"), false)
//              .reduce(com.basho.riak.client.mapreduce.JavascriptFunction.anon(play.modules.riak.RiakMapReduce.orderByReduceString), args, true).submit();
//          if (r.isSuccess()) {
//              return new com.google.gson.Gson().fromJson(r.getBodyAsString(), listType);
//          }
//          else{
//              return null;
//          }
//      } catch (org.json.JSONException e) {
//          e.printStackTrace();
//          return null;
//      }
//  }
}
