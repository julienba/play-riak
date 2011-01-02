import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

import models.riak.Album;
import models.riak.MusicBand;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import play.modules.riak.RiakMP;
import play.modules.riak.RiakModel;
import play.modules.riak.RiakPlugin;
import play.test.UnitTest;

import com.basho.riak.client.RiakObject;
import com.basho.riak.client.mapreduce.JavascriptFunction;
import com.basho.riak.client.response.MapReduceResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


public class MapReduceTest extends UnitTest{

	@Before
	public void setup(){
		MusicBand.deleteAll(MusicBand.class);
		
		// CreateData
		MusicBand mu1 = new MusicBand("TheseArmsAreSnakes", "Post hardcore band featured former member of Botch" );
		mu1.save();
		
		MusicBand mu2 =  new MusicBand("ArcadeFire", "Rock band from Montreal");
		mu2.save();
		
		MusicBand mu3 =  new MusicBand("NeilYoung", "one greatest songwriters and performers");
		mu3.save();
	}
	
	@Test
	public void rawTest(){
				
		// ------------------------------------------------
		// - MAP Reduce
		// ------------------------------------------------
		Type listType = new TypeToken<List<MusicBand>>() {}.getType();
		// Find All		
		/**
{"inputs":"MusicBand",
  "query":[{"map":{"language":"javascript","name":"Riak.mapValuesJson","keep":true}}]
}
		 */
		try {
			MapReduceResponse r = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
			.map(JavascriptFunction.named("Riak.mapValuesJson"), true).submit();
		    if (r.isSuccess()) {
		    	
		    	
		    	List<MusicBand> jsonResult = new Gson().fromJson(r.getBodyAsString(), listType);
		    	assertEquals(3, jsonResult.size());
		        
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}
		
		// Count key
		/**
function mapCount() {
  return [1]
}
		 */
		try {
			MapReduceResponse r = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
				.map(JavascriptFunction.anon("function mapCount(){return [1]}"), false)
				.reduce(JavascriptFunction.named("Riak.reduceSum"),true).submit();
		    if (r.isSuccess()) {
		    	String tmp = r.getBodyAsString();
		    	tmp = tmp.substring(1, tmp.length() - 1);
		    	int res = Integer.parseInt(tmp);
		    	assertEquals(3, res);
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}		
		
		// Slice
		try {
			int[] array = {1,3};
			MapReduceResponse r = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
				.map(JavascriptFunction.named("Riak.mapValuesJson"), false)
				.reduce(JavascriptFunction.named("Riak.reduceSlice"), array, true).submit();
		    if (r.isSuccess()) {
		    	List<MusicBand> jsonResult = new Gson().fromJson(r.getBodyAsString(), listType);
		    	assertEquals(2, jsonResult.size());
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}
		
				
		// Order by field asc
		Object[] args1 = {"name", false};
		String reduceString = "function ( v , args ) {"+
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
		
		JavascriptFunction reduceOrderByFieldFunction = JavascriptFunction.anon(reduceString);
		
		try {
			MapReduceResponse r = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
				.map(JavascriptFunction.named("Riak.mapValuesJson"), false)
				.reduce(reduceOrderByFieldFunction, args1, true).submit();
		    if (r.isSuccess()) {
		    	List<MusicBand> jsonResult = new Gson().fromJson(r.getBodyAsString(), listType);
		    	assertEquals(3, jsonResult.size());
		    	assertEquals("ArcadeFire", jsonResult.get(0).name);
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}		
				
		// Order by field desc
		Object[] args2 = {"name", true};
		try {
			MapReduceResponse r = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
				.map(JavascriptFunction.named("Riak.mapValuesJson"), false)
				.reduce(reduceOrderByFieldFunction, args2, true).submit();
		    if (r.isSuccess()) {
		    	List<MusicBand> jsonResult = new Gson().fromJson(r.getBodyAsString(), listType);
		    	assertEquals(3, jsonResult.size());
		    	assertEquals("TheseArmsAreSnakes", jsonResult.get(0).name);
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}
		
		try{
			//do what you want to do before sleeping
			Thread.currentThread().sleep(2000);//sleep for 1000 ms
			//do what you want to do after sleeptig
		}
		catch(Exception e){
			e.printStackTrace(); 
		}		
		
		MusicBand mb = new MusicBand("Justice", "Electro buzz band");
		mb.save();		
				
		// Order by creation Date asc		
		try {
			MapReduceResponse r = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
				.map(JavascriptFunction.anon(RiakMP.orderByCreationDateMapString), false)
				.reduce(JavascriptFunction.anon(RiakMP.orderByCreateDateAscReduceString), false)
				.reduce(JavascriptFunction.anon(RiakMP.cleanReduce), true).submit();
		    if (r.isSuccess()) {
		    	List<MusicBand> jsonResult = new Gson().fromJson(r.getBodyAsString(), listType);
		    	assertEquals(4, jsonResult.size());
		    	assertEquals("Justice", jsonResult.get(3).name);
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}
		
		// Order by creation Date desc
		try {
			MapReduceResponse r4 = RiakPlugin.riak.mapReduceOverBucket("MusicBand")
				.map(JavascriptFunction.anon(RiakMP.orderByCreationDateMapString), false)
				.reduce(JavascriptFunction.anon(RiakMP.orderByCreateDateDescReduceString), false)
				.reduce(JavascriptFunction.anon(RiakMP.cleanReduce), true).submit();
		    if (r4.isSuccess()) {
		    	List<MusicBand> jsonResult = new Gson().fromJson(r4.getBodyAsString(), listType);
		    	assertEquals(4, jsonResult.size());
		    	assertEquals("Justice", jsonResult.get(0).name);
		    }else
		    	assertFalse(true);	
		} catch (JSONException e) {
			e.printStackTrace();
			assertFalse(true);
		}		
	}
	
	@Test
	public void riakMPTest(){
		// Count
		long res = RiakMP.count(MusicBand.class);
		long nb = 3;
		assertEquals(nb, res);
		
		// Fetch
		Type listType = new TypeToken<List<MusicBand>>() {}.getType();
		List<MusicBand> l = MusicBand.fetch(MusicBand.class, listType, 1, 3);
		assertNotNull(l);
		assertEquals(2, l.size());
		
		// orderByName desc
		List<MusicBand> orderList = MusicBand.orderBy(MusicBand.class, "name", true, listType);
		assertNotNull(orderList);
		assertEquals(3, orderList.size());
		assertEquals("TheseArmsAreSnakes", orderList.get(0).name);
		
		try{
			Thread.currentThread().sleep(2000);
		}
		catch(Exception e){
			e.printStackTrace(); 
		}		
		
		MusicBand mb = new MusicBand("Justice", "Electro buzz band");
		mb.save();	
		
		// Order by modifyDate asc
		List<MusicBand> muOrderByDate = MusicBand.findOrderByDate(MusicBand.class, listType);
		assertNotNull(muOrderByDate);
		assertEquals(4, muOrderByDate.size());
		assertEquals("Justice", muOrderByDate.get(3).name);
		
		// Order by modifyDate desc
		List<MusicBand> muOrderByDateDesc = MusicBand.findOrderByDate(MusicBand.class, listType, true);
		assertNotNull(muOrderByDateDesc);
		assertEquals(4, muOrderByDateDesc.size());
		assertEquals("Justice", muOrderByDateDesc.get(0).name);		
	}
	
	public void miscQuery(){
		
		/**
		 * Basic example
		 */
		
		// retrieve all element
		List<MusicBand> list = MusicBand.findAll(MusicBand.class);
		
		// find one element with that key
		MusicBand m = MusicBand.find(MusicBand.class, "key");
		m.save();
		// use raw riak object
		RiakObject riakObject = m.getObj();
		
		//delete one element
		m.delete();
		
		// find all keys for one bucket
		Collection<String> allKeys = MusicBand.findKeys(MusicBand.class);
		
		/**
		 * link example
		 */
		// add link
		m.addLink(Album.class, "key", "tag");
		
		// simple link walking
		List<RiakModel> links = m.getLink();
		
		/**
		 * Map/reduce example
		 */		
		
		// count element with map/reduce query
		long nbElement = RiakMP.count(MusicBand.class);
		
		// retrieve order element by field 
		Type listType = new TypeToken<List<MusicBand>>() {}.getType();
		List<MusicBand> orderList = MusicBand.orderBy(MusicBand.class, "name", true, listType);
		
		// find element order by last edit date
		List<MusicBand> orderByDateList = MusicBand.findOrderByDate(MusicBand.class, listType);
	}
}
