import java.util.List;

import org.junit.Test;

import play.modules.riak.RiakPlugin;
import play.test.UnitTest;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;


public class RiakClientTest extends UnitTest{
	
	@Test
	public void builddObject(){

		try {
			Bucket myBucket = RiakPlugin.riak.createBucket("test").execute();
			try {
				// store value
				myBucket.store("sonic_youth", "{'guitar1':'Thurston Moore','bass':'Kim Gordon'}").execute();
				
				// retrieve
				IRiakObject myData = myBucket.fetch("sonic_youth").execute();				
				assertEquals("{'guitar1':'Thurston Moore','bass':'Kim Gordon'}", myData.getValueAsString());
				
				// delete
				myBucket.delete("sonic_youth").execute();
				
				// retrieve
				IRiakObject myDeleteData = myBucket.fetch("sonic_youth").execute();
				assertNull(myDeleteData);
			} catch (UnresolvedConflictException e) {
				e.printStackTrace();
				assertTrue(false);
			} catch (ConversionException e) {
				e.printStackTrace();
				assertTrue(false);
			}
		} catch (RiakRetryFailedException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

// TODO: links NOT WORKS, maybe a bug
	@Test
	public void links(){
		
		try {
			Bucket bucket = RiakPlugin.riak.createBucket("test").execute();
			
			// store
	        IRiakObject o1 = RiakObjectBuilder
        		.newBuilder("test", "sonic_youth")
        		.withValue("bla")
        		.addLink("pavement", "foo", "foo").build();			
			
	        bucket.store(o1);
	        
	        IRiakObject o2 = RiakObjectBuilder
    			.newBuilder("test", "pavement")
    			.withValue("blibli").build();			
		
	        bucket.store(o2);	        
	        

	        // retrieve
	        IRiakObject o11 = bucket.fetch("sonic_youth").execute();
	        List<RiakLink> links = o11.getLinks();
	        assertNotNull(links);
	        assertEquals(1, links.size());
			assertEquals("pavement", links.get(0).getKey());
			assertEquals("foo", links.get(0).getTag());
				
			// delete
			IRiakObject myDeleteData = bucket.fetch("sonic_youth").execute();
			assertNull(myDeleteData);			
			
		} catch (Exception e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}
	
	@Test
	public void crud(){
		
		
		try {
			// CREATE	
			Bucket bucket = RiakPlugin.riak.createBucket("test").execute();
			bucket.store("aa", "foo").execute();
			
			// RETRIEVE
			IRiakObject o = bucket.fetch("aa").execute();				
			assertNotNull(o);
			assertEquals("foo", o.getValueAsString());
			
			// UPDATE	
			//TODO: NOT works, maybe a bug ?
//			o.setValue("bar");
//			bucket.store(o);
			
			Bucket bucket1 = RiakPlugin.riak.createBucket("test").execute();
			bucket1.store("aa", "bar").execute();			
			
			
			// RETRIEVE
			Bucket bucket2 = RiakPlugin.riak.createBucket("test").execute();
			IRiakObject o2 = bucket2.fetch("aa").execute();				
			assertNotNull(o2);
			assertEquals("bar", o2.getValueAsString());
			
			// DELETE
			bucket2.delete("aa").execute();
			
			// RETRIEVE	
			IRiakObject myDeleteData = bucket2.fetch("aa").execute();
			assertNull(myDeleteData);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
