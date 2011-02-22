package models.riak;

import play.modules.riak.RiakEntity;
import play.modules.riak.RiakModel;

@RiakEntity(key="name")
public class MusicBand extends RiakModel{

	public String name;
	public String description;
	
	public MusicBand(String name, String description) {
		super();
		this.name = name;
		this.description = description;
	}
	
	public static MusicBand find12(String bucket, String key){
		
		try {
			com.basho.riak.pbc.RiakObject[] ro = play.modules.riak.RiakPlugin.riak.fetch(bucket, key);
			if(ro.length > 0){
				com.basho.riak.pbc.RiakObject o = ro[0];
				MusicBand e = (MusicBand)new com.google.gson.Gson().fromJson(o.getValue().toStringUtf8(), MusicBand.class);
				e.setObj(o);
				return e;
			}			
		} catch (java.io.IOException e1) {
			e1.printStackTrace();
		}
		return null;		
	}
	
	public static java.util.List findAll2(String bucket){
		java.util.Collection keys = MusicBand.findKeys(bucket);
		java.util.List result = new java.util.ArrayList();
		for (java.util.Iterator iterator = keys.iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			result.add(find(bucket,key));
		}
		return result;
	}
	
	
	@Override
	public String toString() {
		return "MusicBand [description=" + description + ", name=" + name + "]";
	}		
}
