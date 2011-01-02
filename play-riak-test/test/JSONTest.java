import models.riak.Album;

import org.junit.Test;

import play.test.UnitTest;

import com.google.gson.Gson;


public class JSONTest extends UnitTest{

	@Test
	public void serializeObject(){
		Album a = new Album("Fluorescent Black", 2009);
		Gson gson = new Gson();
		String json = gson.toJson(a);
		assertEquals("{\"name\":\"Fluorescent Black\",\"year\":2009}", json);
	}
}
