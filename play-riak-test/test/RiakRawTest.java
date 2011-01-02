import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import play.libs.WS;
import play.libs.WS.HttpResponse;
import play.libs.WS.WSRequest;
import play.modules.riak.RiakPlugin;
import play.test.UnitTest;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * Make many test for use raw api describe here : https://wiki.basho.com/display/RIAK/REST+API
 *
 */
public class RiakRawTest extends UnitTest{

	public static String BASE_URL = "http://127.0.0.1:8091/";
		
	// curl -H "Accept: text/plain" http://127.0.0.1:8091/stats
	@Test
	public void stat(){
		
		HttpResponse response = WS.url(RiakPlugin.RIAK_URL + "/" + RiakPlugin.RIAK_URL_STATS).get();
		
		int status = response.getStatus();
		assertEquals(200, status);	
		assertEquals("application/json", response.getContentType());
	}
	
//	@Test
//	public void CRUD(){
//			
//		// Store a new object without a key
//		// curl -v -d 'this is a test' -H "Content-Type: text/plain" http://127.0.0.1:8098/riak/test		
//		Map<String, Object> params = new HashMap<String,Object>();
//		params.put("", "this is a test");
//		HttpResponse response2 = WS.url(BASE_URL + "riak/test").params(params).post();
//		//System.out.println("RiakRawTest.CRUD()::response2: " + response2.getString());
//		int status2 = response2.getStatus();
//		assertEquals(201, status2);
//		
//		
//		// Store a new or existing object with a key
//		// curl -v -X PUT -d '{"bar":"baz"}' -H "Content-Type: application/json" -H "X-Riak-Vclock: a85hYGBgzGDKBVIszMk55zKYEhnzWBlKIniO8mUBAA==" http://127.0.0.1:8098/riak/test/doc?returnbody=true
//		// return : {"bar":"baz"} , status: 200
//		WSRequest wsRequest = WS.url(BASE_URL + "riak/test/doc?returnbody=true");
//		wsRequest.headers.put("X-Riak-Vclock", "a85hYGBgzGDKBVIszMk55zKYEhnzWBlKIniO8mUBAA==");
//		wsRequest.headers.put("content-type", "application/json");
//		Map<String, Object> params2 = new HashMap<String,Object>();
//		params2.put("foo", "bar");		
//		//wsRequest.parameters = params2;
//		wsRequest.params(params2);
//		HttpResponse response3 = wsRequest.post();
//		int status3 = response3.getStatus();
//		assertEquals(200, status3);
//	}
	
	@Test
	public void listKey(){
		WSRequest wsRequest = WS.url(BASE_URL + "riak/test?keys=true");
		Map<String, Object> params = new HashMap<String,Object>();
		params.put("keys", true);
		params.put("props", "true");
		wsRequest.headers.put("X-Riak-Vclock", "a85hYGBgzGDKBVIszMk55zKYEhnzWBlKIniO8mUBAA==");
		wsRequest.headers.put("Content-Type", "application/json");
		HttpResponse response = wsRequest.get();
		
		// Create list
		String json = response.getString();
		assertNotSame("", json);
	}
}
