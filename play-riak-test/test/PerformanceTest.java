import java.io.IOException;

import models.riak.MusicBand;

import org.junit.Before;
import org.junit.Test;

import play.Logger;
import play.Play;
import play.test.UnitTest;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;


public class PerformanceTest extends UnitTest{

	public int NB = 10;
	
	@Before
	public void setUp(){
		MusicBand.deleteAll("MusicBand");
	}
	
	
	@Test
	public void rawClient(){
		
		long beginning = System.currentTimeMillis();
		RiakClient riak = new RiakClient(Play.configuration.getProperty("riak.url"));	
		RiakObject o = null;
		for (int i = 0; i < NB; i++) {
			o = new RiakObject("MusicBand", "name" + i);
			o.setValue("bla");
			riak.store(o);			
		}
		
		long ending = System.currentTimeMillis();
		long interval = ending - beginning;
		Logger.warn("RawClient: %d insert during: %d ms", NB, interval);
		assertTrue(true);
	}
	
	@Test
	public void officialPBClient(){
		String host = Play.configuration.getProperty("riak.protobuf.url");
		int port = Integer.valueOf(Play.configuration.getProperty("riak.protobuf.port"));
		try {
			com.basho.riak.pbc.RiakClient rc = new com.basho.riak.pbc.RiakClient(host, port);
			com.basho.riak.pbc.RiakObject o = null;
			
			long beginning = System.currentTimeMillis();
			
			for (int i = 0; i < NB; i++) {
				o = new com.basho.riak.pbc.RiakObject("MusicBand", "name"+i, "bla");
				rc.store(o);			
			}
			
			long ending = System.currentTimeMillis();
			long interval = ending - beginning;
			Logger.warn("RawClient: %d insert during: %d ms", NB, interval);
			assertTrue(true);			
			
		} catch (IOException e) {
			assertFalse(true);
			e.printStackTrace();
		}
	}
	
	@Test
	public void entityClient(){
		long beginning = System.currentTimeMillis();

		for (int i = 0; i < NB; i++) {
			MusicBand mb = new MusicBand("name1", "bla");
			mb.save();
		}
		
		long ending = System.currentTimeMillis();
		long interval = ending - beginning;
		
		Logger.warn("EntityClient: %d insert during: %d ms", NB, interval);
		assertTrue(true);		
	}	
}
