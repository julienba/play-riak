import java.io.IOException;
import java.util.List;

import models.riak.MusicBand;

import org.junit.Test;

import play.Play;
import play.modules.riak.RiakPlugin;
import play.test.UnitTest;

import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.pbc.mapreduce.MapReduceResponse;
import com.google.protobuf.ByteString;


public class RiakPbTest extends UnitTest{

	@Test
	public void doSomethingWitkClient(){
		
		String url = Play.configuration.getProperty("riak.protobuf.url");
		int port = Integer.valueOf(Play.configuration.getProperty("riak.protobuf.port"));
		
		try {
			RiakClient riak = new RiakClient(url, port);
			riak.ping();
			
			// Create
			RiakObject ro = new RiakObject("test", "key-pb", "value1");
			ro.addLink("tag1", "testtesttttttttttttttttttt", "key-pb-link");
			assertEquals(1, ro.getLinks().size());
			riak.store(ro);
			
			// retrieve
			RiakObject[] res = riak.fetch("test", "key-pb");

			// check
			assertEquals(1, res.length);
			assertEquals("value1", res[0].getValue().toStringUtf8());
			assertEquals(1, res[0].getLinks().size());
			
			
		} catch (IOException e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}
	
	@Test
	public void find(){
		MusicBand mb = new MusicBand("ElectricWizard", "doooooooooom");
		mb.save();
		
		mb = MusicBand.find("MusicBand", "ElectricWizard");
		assertNotNull(mb);
		assertEquals("ElectricWizard", mb.name);
		assertEquals("doooooooooom", mb.description);
		
		List<MusicBand> list = MusicBand.findAll("MusicBand");
		assertNotNull(list);
		assertNotSame(0,list.size());
		
		// Basic Map reduce
		try {
			
			MapReduceResponseSource mrs = RiakPlugin.riak.mapReduce("{\"inputs\": \"MusicBand\", "+
					 "\"query\":"+
					    "[{\"map\": {\"arg\": null,"+
					              "\"name\": \"Riak.mapValuesJson\","+
					              "\"language\": \"javascript\","+
					              "\"keep\": true}}]}", new RequestMeta().contentType("application/json"));
			
			while(mrs.hasNext()){
				MapReduceResponse mr = mrs.next();
				ByteString bs = mr.getContent();
				if(bs != null && !bs.isEmpty())
					System.out.println("RiakPbTest.find()::res: " + bs.toStringUtf8());
			}
		} catch (IOException e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}
}
