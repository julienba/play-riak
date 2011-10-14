import java.util.List;
import java.util.Map.Entry;

import models.riak.Album;
import models.riak.MusicBand;
import models.riak.Year;

import org.junit.Test;

import play.modules.riak.RiakKey;
import play.modules.riak.RiakModel;
import play.modules.riak.RiakPath;
import play.modules.riak.RiakPlugin;
import play.modules.riak.RiakUtil;
import play.test.UnitTest;

import com.basho.riak.client.IRiakObject;


public class RiakEntityTest extends UnitTest{

	/**
	 * curl -v -X GET http://127.0.0.1:8098/riak/MusicBand?keys=true
	 */
	@Test
	public void saveAndFind(){
		
		// Delete all
		Iterable<String> keys = MusicBand.findKeys("MusicBand");
		for(String key : keys){
			MusicBand.delete("MusicBand", key);
		}
		
		Iterable<String> keysAfter = MusicBand.findKeys("MusicBand");
		int cpt = 0;
		for (String string : keysAfter)
			cpt++;
		assertEquals(0, cpt);
		
		// Save
		MusicBand mu = new MusicBand("SonicYouth", "Best band ever");
		assertTrue(mu.save());
		
		MusicBand mu2 = new MusicBand("Dillinger_escape", "Insane");
		assertTrue(mu2.save());
		
		Iterable<String> keysAfter2 = MusicBand.findKeys("MusicBand");
		for (String string : keysAfter2){
			cpt++;
		}
		assertEquals(2, cpt);		
		
		// Find
		MusicBand mu3 = MusicBand.find("MusicBand", "SonicYouth");
		assertNotNull(mu3);
		assertEquals("SonicYouth", mu3.name);
		assertEquals("Best band ever", mu3.description);

		assertNotNull(mu3.getUserMeta());
		Iterable<Entry<String, String>> metas = mu3.getUserMeta();
		cpt = 0;
		assertNotNull(metas);
		for (Entry<String, String> entry : metas) {
			cpt++;
		}
		assertEquals(0, cpt);
		
		// Edit
		mu3.description = "Sonic Youth is an American rock band from New York City, formed in 1981.";
		assertTrue(mu3.save());
		
		// Refind and check
		MusicBand mu4 = MusicBand.find("MusicBand", "SonicYouth");
		assertEquals("Sonic Youth is an American rock band from New York City, formed in 1981.", mu4.description);
		
		
		List<MusicBand> list = MusicBand.findAll("MusicBand");
		assertNotNull(list);
		assertEquals(2, list.size());
		
	}
	
	@Test
	public void pathTest(){
		
		// Check content of path map
		assertEquals(3, RiakPlugin.bucketMap.size());
	
		RiakKey key1 = RiakPlugin.bucketMap.get("NIMPORTEQUOI");
		assertNull(key1);

		RiakKey key2 = RiakPlugin.bucketMap.get("models.riak.MusicBand");
		assertNotNull(key2);
		
		assertEquals("MusicBand", key2.getBucket());
		assertEquals("name", key2.getKey());
		
		RiakKey key3 = RiakPlugin.bucketMap.get("models.riak.Album");
		assertNotNull(key3);
		
		RiakKey key4 = RiakPlugin.bucketMap.get("models.riak.Year");
		assertNotNull(key4);		
		
		
		// Resolve path
		Album a = new Album("Daydream_Nation", 1988);
		RiakPath path = a.getPath();

		assertNotNull(path);
		assertEquals("AlbumTest", path.getBucket());
		assertEquals("Daydream_Nation", path.getValue());
	}
	
	@Test
	public void resave(){
		Album.delete("AlbumTest", "DaydreamNation");
		
		Album a = new Album("DaydreamNation", 1988);
		assertTrue(a.save());
		
		a.addLink("MusicBand", "SonicYouth", "author");
		assertTrue(a.save());
	
		Album aa = Album.find("AlbumTest", "DaydreamNation");
		assertNotNull(aa);
		
		IRiakObject o = aa.getObj();
		assertNotNull(o);
		
		// Links stuff
//		assertEquals(1, o.getLinks().size());
//		
//		assertEquals(1, aa.getRawLink().size());
//		assertEquals(1, aa.getRawLinksByTag("author").size());
//		assertEquals(0, aa.getRawLinksByTag("NIMP").size());
//		
//		List<RiakModel> list = aa.getLink();
//		
//		assertEquals(1, list.size());
//		
//		MusicBand mb = (MusicBand)list.get(0);
//		assertNotNull(mb);
//		assertEquals("SonicYouth", mb.name);
//		
//		assertEquals(1, aa.getLinksByTag("author").size());
//		assertEquals(0, aa.getLinksByTag("NIMP").size());
	}
	
	@Test
	public void deleteAll(){		
		RiakUtil.deleteAllDefineBucket();
		Iterable<String> keys = MusicBand.findKeys("Musicband");
		
		int cpt = 0;
		for (String string : keys) {
			cpt++;
		}
		assertEquals(0, cpt);
		
		keys = Year.findKeys("Year");
		for (String string : keys) {
			cpt++;
		}
		assertEquals(0, cpt);		
		
		
		keys = Year.findKeys("AlbumTest");
		for (String string : keys) {
			cpt++;
		}
		assertEquals(0, cpt);
		
	}
	
	@Test
	public void saveAndfindWithoutBucketName(){
		// Delete all
		Iterable<String> keys = MusicBand.findKeys(MusicBand.class);
		for(String key : keys){
			MusicBand.delete(MusicBand.class, key);
		}
		
		// Save
		MusicBand mu = new MusicBand("SonicYouth", "Best band ever");
		assertTrue(mu.save());
		
		MusicBand mu2 = new MusicBand("Dillinger_escape", "Insane");
		assertTrue(mu2.save());
		
		Iterable<String> keysAfter2 = MusicBand.findKeys(MusicBand.class);
		int cpt = 0;
		for (String string : keysAfter2) {
			cpt++;
		}
		assertEquals(2, cpt);				
		
		// Find
		MusicBand mu3 = MusicBand.find(MusicBand.class, "SonicYouth");
		assertNotNull(mu3);
		assertEquals("SonicYouth", mu3.name);
		assertEquals("Best band ever", mu3.description);
		assertNotNull(mu3.getUserMeta());
		
		// Edit
		mu3.description = "Sonic Youth is an American rock band from New York City, formed in 1981.";
		assertTrue(mu3.save());
		
		// Refind and check
		MusicBand mu4 = MusicBand.find(MusicBand.class, "SonicYouth");
		assertEquals("Sonic Youth is an American rock band from New York City, formed in 1981.", mu4.description);
		
		List<MusicBand> list = MusicBand.findAll(MusicBand.class);
		assertNotNull(list);
		assertEquals(2, list.size());
	}
}
