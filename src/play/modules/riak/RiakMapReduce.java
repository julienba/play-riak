package play.modules.riak;

import java.util.List;

import org.json.JSONException;

import play.Logger;

import com.basho.riak.client.mapreduce.JavascriptFunction;
import com.basho.riak.client.mapreduce.MapReduceFunction;
import com.basho.riak.client.response.MapReduceResponse;

public class RiakMapReduce {

	// Contributed By: Francisco Treacy
	// http://contrib.basho.com/sorting-by-field.html
	public static String orderByReduceString = "function ( v , args ) {"+
		"var field = args[0];"+
		"var reverse = args[1];"+
		"v.sort(function(a, b) {"+
	        "if (reverse) {"+
	            "var _ref = [b, a];"+
	            "a = _ref[0];"+
	            "b = _ref[1];"+
	        "}"+
	        "if (((typeof a === \"undefined\" || a === null) ? undefined :"+
	            "a[field]) < ((typeof b === \"undefined\" || b === null) ? undefined :"+
	            "b[field])) {"+
	                "return -1;"+
	        "} else if (((typeof a === \"undefined\" || a === null) ? undefined :"+
	            "a[field]) === ((typeof b === \"undefined\" || b === null) ? undefined :"+
	            "b[field])) {"+
	                "return 0;"+
	            "} else if (((typeof a === \"undefined\" || a === null) ? undefined :"+
	                "a[field]) > ((typeof b === \"undefined\" || b === null) ? undefined :"+
	                "b[field])) {"+
	            "return 1;"+
	        "}"+
	    "});"+            				
		"return v"+
	"}";	
	
	
	// see http://siculars.posterous.com/using-riaks-mapreduce-for-sorting
	public static String orderByCreationDateMapString = 
		"function(v, keydata, args) {" +
			"if (v.values) {" +
				"var ret = [];" +
				"o = Riak.mapValuesJson(v)[0];" +
				"o.lastModifiedParsed = Date.parse(v['values'][0]['metadata']['X-Riak-Last-Modified']);" +
				"o.key = v['key'];ret.push(o);return ret;" +
			"} else {" +
				"return [];" +
			"}" +
		"}";
	
	public static final String orderByCreateDateAscReduceString = 
		"function ( v , args ) {" +
			"v.sort ( function(a,b) {" +
				"return a['lastModifiedParsed'] - b['lastModifiedParsed']" +
			"} );" +
			"return v" +
		"}";
	
	public static final String orderByCreateDateDescReduceString = 
		"function ( v , args ) {" +
			"v.sort ( function(a,b) {" +
				"return b['lastModifiedParsed'] - a['lastModifiedParsed'] " +
			"} );" +
			"return v" +
		"}";
	
	// add loop in reduce function because java is less flexible than javascript ( TODO: find a better way)
	public static final String cleanReduce = "function ( v , args ) {" +
			"for(var i=0;i<v.length;i++)v[i].lastModifiedParsed = null;"+
			"return v" +
		"}";	
	
	public static long count(Class clazz){
//		try {
//			MapReduceResponse r2 = RiakPlugin.riak.mapReduceOverBucket(RiakPlugin.getBucketName(clazz))
//				.map(JavascriptFunction.anon("function mapCount(){return [1]}"), false)
//				.reduce(JavascriptFunction.named("Riak.reduceSum"),true).submit();
//		    if (r2.isSuccess()) {
//		    	String tmp = r2.getBodyAsString();
//		    	tmp = tmp.substring(1, tmp.length() -1);
//		    	int res = Integer.parseInt(tmp);
//		    	return res;
//		    }else{
//		    	Logger.error("Error during count for class %s", clazz.getName());
//		    }
//		} catch (JSONException e) {
//			Logger.error("Error during count for class %s", clazz.getName());
//			e.printStackTrace();
//		}		
		
		return -1;
	}
}
