package models.riak;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;

import com.basho.riak.client.mapreduce.JavascriptFunction;
import com.basho.riak.client.response.MapReduceResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import javassist.CtMethod;
import play.Logger;
import play.modules.riak.RiakEntity;
import play.modules.riak.RiakMP;
import play.modules.riak.RiakModel;
import play.modules.riak.RiakPlugin;

@RiakEntity(key="name")
public class MusicBand extends RiakModel{

	public String name;
	public String description;
	
	public MusicBand(String name, String description) {
		super();
		this.name = name;
		this.description = description;
	}
	
	@Override
	public String toString() {
		return "MusicBand [description=" + description + ", name=" + name + "]";
	}		
}
