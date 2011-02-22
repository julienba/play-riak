import java.util.Collection;
import java.util.List;

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

import com.basho.riak.pbc.RiakObject;


public class RiakEntityTest extends UnitTest{

	@Test
	public void saveAndFind(){
		
		// Delete all
		Collection<String> keys = MusicBand.findKeys("MusicBand");
		for(String key : keys){
			MusicBand.delete("MusicBand", key);
		}
		
		Collection<String> keysAfter = MusicBand.findKeys("MusicBand");
		assertEquals(0, keysAfter.size());
		
		// Save
		MusicBand mu = new MusicBand("SonicYouth", "Best band ever");
		assertTrue(mu.save());
		
		MusicBand mu2 = new MusicBand("Dillinger_escape", "Insane");
		assertTrue(mu2.save());
		
		Collection<String> keysAfter2 = MusicBand.findKeys("MusicBand");
		assertEquals(2, keysAfter2.size());		
		
		// Find
		MusicBand mu3 = MusicBand.find("MusicBand", "SonicYouth");
		assertNotNull(mu3);
		assertEquals("SonicYouth", mu3.name);
		assertEquals("Best band ever", mu3.description);

		assertNotNull(mu3.getUserMeta());
		assertEquals(0, mu3.getUserMeta().size());
		
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
		
		RiakObject o = aa.getObj();
		assertNotNull(o);
		
		assertEquals(1, o.getLinks().size());
		
		assertEquals(1, aa.getRawLink().size());
		assertEquals(1, aa.getRawLinksByTag("author").size());
		assertEquals(0, aa.getRawLinksByTag("NIMP").size());
		
		List<RiakModel> list = aa.getLink();
		
		assertEquals(1, list.size());
		
		MusicBand mb = (MusicBand)list.get(0);
		assertNotNull(mb);
		assertEquals("SonicYouth", mb.name);
		
		assertEquals(1, aa.getLinksByTag("author").size());
		assertEquals(0, aa.getLinksByTag("NIMP").size());
	}
	
	@Test
	public void deleteAll(){		
		RiakUtil.deleteAllDefineBucket();
		Collection<String> keys = MusicBand.findKeys("Musicband");
		assertEquals(0, keys.size());
		
		assertEquals(0, Year.findKeys("Year").size());
		assertEquals(0, Album.findKeys("AlbumTest").size());
		
	}
	
	@Test
	public void saveAndfindWithoutBucketName(){
		// Delete all
		Collection<String> keys = MusicBand.findKeys(MusicBand.class);
		for(String key : keys){
			MusicBand.delete(MusicBand.class, key);
		}
		
		Collection<String> keysAfter = MusicBand.findKeys(MusicBand.class);
		assertEquals(0, keysAfter.size());
		
		// Save
		MusicBand mu = new MusicBand("SonicYouth", "Best band ever");
		assertTrue(mu.save());
		
		MusicBand mu2 = new MusicBand("Dillinger_escape", "Insane");
		assertTrue(mu2.save());
		
		Collection<String> keysAfter2 = MusicBand.findKeys(MusicBand.class);
		assertEquals(2, keysAfter2.size());		
		
		// Find
		MusicBand mu3 = MusicBand.find(MusicBand.class, "SonicYouth");
		assertNotNull(mu3);
		assertEquals("SonicYouth", mu3.name);
		assertEquals("Best band ever", mu3.description);
		assertNotNull(mu3.getUserMeta());
		assertEquals(0, mu3.getUserMeta().size());
		
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
