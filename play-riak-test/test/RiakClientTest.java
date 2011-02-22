import java.util.Collection;
import java.util.List;

import org.junit.Test;

import play.Play;
import play.modules.riak.RiakPlugin;
import play.mvc.Http;
import play.test.UnitTest;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.WalkResponse;


public class RiakClientTest extends UnitTest{

	@Test
	public void builddObject(){
		RiakClient riak = new RiakClient(Play.configuration.getProperty("riak.url"));
		RiakObject o = null;
		o = new RiakObject("test", "sonic_youth");
		riak.store(o);
		
		// retrieve it and set value
	    FetchResponse r = riak.fetch("test", "sonic_youth");
	    assertTrue(r.hasObject());
	    if (r.hasObject()){
	        o = r.getObject();
	        o.setValue("{'guitar1':'Thurston Moore','bass':'Kim Gordon'}");
	        riak.store(o);
	    }

	    // retrieve with value
	    FetchResponse r2 = riak.fetch("test", "sonic_youth");
	    assertTrue(r.hasObject());
	    if (r2.hasObject()){
	        o = r2.getObject();
	        assertEquals("{'guitar1':'Thurston Moore','bass':'Kim Gordon'}", o.getValue());
	    }
	    
	    // delete
		HttpResponse response = riak.delete("test", "sonic_youth");
	    assertEquals(Http.StatusCode.NO_RESPONSE, response.getStatusCode());
	    
		HttpResponse response2 = riak.delete("test", "sonic_youth");
		assertEquals(Http.StatusCode.NOT_FOUND, response2.getStatusCode());	    

	}
	
	@Test
	public void find(){
		RiakClient riak = new RiakClient(Play.configuration.getProperty("riak.url"));
		BucketResponse r = riak.listBucket("test");
		Collection<String> keys = r.getBucketInfo().getKeys();
		assertNotNull(keys);
	}
	
	@Test
	public void links(){
		RiakClient riak = new RiakClient(Play.configuration.getProperty("riak.url"));
		RiakObject o = null;
		
		o = new RiakObject("test", "sonic_youth");
		o.setValue("blablabla");
		o.addLink(new RiakLink("test", "pavement", "foo"));
		riak.store(o);
		
		o = new RiakObject("test", "pavement");
		o.setValue("blibliblibli");
		o.addLink(new RiakLink("test", "sonic_youth", "bar"));
		riak.store(o);
		
		
		
		// Get response
		FetchResponse fr = riak.fetch("test", "sonic_youth");
		if(fr.hasObject()){
			o = fr.getObject();
			assertEquals("blablabla", o.getValue());
			List<RiakLink> links = o.getLinks();
			
			assertEquals(1, links.size());
			assertEquals("pavement", links.get(0).getKey());
			assertEquals("foo", links.get(0).getTag());
		}
		
		// Walking
		WalkResponse wr = riak.walk("test", "sonic_youth", "test,_,1");
		assertTrue(wr.isSuccess());
		
	    if (wr.isSuccess()) {
	        List<? extends List<RiakObject>> steps = wr.getSteps();
	        assertEquals(1, steps.size());
	        
	        List<RiakObject> step = steps.get(0);
	        assertEquals(1, step.size());
	        RiakObject obj = step.get(0);
	        assertEquals("blibliblibli", obj.getValue());
	    }
	}
	
	@Test
	public void crud(){
		RiakClient riak = new RiakClient(Play.configuration.getProperty("riak.url"));
		RiakObject o = null;
		
		// CREATE
		o = new RiakObject("test", "sonic_youth");
		o.setValue("foo");
		riak.store(o);
		o = null;
		assertNull(o);
		
		// RETRIEVE
		FetchResponse r1 = riak.fetch("test", "sonic_youth");		
		o = r1.getObject();
		assertEquals("foo", o.getValue());
		
		// UPDATE
		o.setValue("bar");
		StoreResponse storeResponse = riak.store(o);
		assertEquals("bar", storeResponse.getBodyAsString());
		o = null;
		assertNull(o);

		// RETRIEVE
		FetchResponse r2 = riak.fetch("test", "sonic_youth");		
		o = r2.getObject();
		assertEquals("bar", o.getValue());
				
		// DELETE
		o.delete();
				
		// RETRIEVE
		FetchResponse r3 = riak.fetch("test", "sonic_youth");		
		assertFalse(r3.hasObject());		
		
	}
}
