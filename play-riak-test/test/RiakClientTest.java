import static play.modules.riak.RiakPlugin.riak;

import org.junit.Test;

import play.modules.riak.RiakPlugin;
import play.test.UnitTest;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;


public class RiakClientTest extends UnitTest{
	
	/**
	 * CURL SAMPLE:
	 * curl -v -X GET http://127.0.0.1:8098/riak/test/sonic_youth
	 */
	@Test
	public void builddObject(){

		try {
			Bucket myBucket = RiakPlugin.riak.createBucket("test").execute();
			try {
				// store value
				IRiakObject obj = myBucket.store("sonic_youth", "{'guitar1':'Thurston Moore','bass':'Kim Gordon'}").returnBody(true).execute();
				assertNotNull(obj);
				
				// retrieve
				IRiakObject myData = myBucket.fetch("sonic_youth").execute();		
				assertEquals("{'guitar1':'Thurston Moore','bass':'Kim Gordon'}", myData.getValueAsString());
				
				
				// delete
				try {
					myBucket.delete("sonic_youth").execute();
				} catch (RiakException e1) {
					e1.printStackTrace();
					assertTrue(false);
				}
				
				//fetch client
				try {
					Iterable<String> keys = RiakPlugin.riak.fetchBucket("test").execute().keys();
					int nbKeys = 0;
					for (String string : keys) {
						nbKeys++;
					}
					assertEquals(1, nbKeys);
				} catch (RiakException e) {
					e.printStackTrace();
					assertFalse(true);
				}
				
				
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

//	// TODO: links NOT WORKS actually
//	@Test
//	public void links(){
//		
//		try {
//			Bucket bucket = RiakPlugin.riak.createBucket("test").execute();
//			
//			// store
//	        IRiakObject o1 = RiakObjectBuilder
//        		.newBuilder("test", "sonic_youth")
//        		.withValue("bla")
//        		.addLink("pavement", "foo", "foo").build();			
//			
//	        bucket.store(o1);
//	        
//	        IRiakObject o2 = RiakObjectBuilder
//    			.newBuilder("test", "pavement")
//    			.withValue("blibli").build();			
//		
//	        bucket.store(o2);	        
//	        
//
//	        // retrieve
//	        IRiakObject o11 = bucket.fetch("sonic_youth").execute();
//	        List<RiakLink> links = o11.getLinks();
//	        assertNotNull(links);
//	        assertEquals(1, links.size());
//			assertEquals("pavement", links.get(0).getKey());
//			assertEquals("foo", links.get(0).getTag());
//				
//			// delete
//			IRiakObject myDeleteData = bucket.fetch("sonic_youth").execute();
//			assertNull(myDeleteData);			
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//			assertFalse(true);
//		}
//	}
	

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
			assertFalse(true);
		}
	}
}
