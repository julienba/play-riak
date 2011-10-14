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
	
//	public static java.util.List findAll(String bucket){
//		java.lang.Iterable keys = MusicBand.findKeys(bucket);
//		java.util.List result = new java.util.ArrayList();
//		for (java.util.Iterator iterator = keys.iterator(); iterator.hasNext();) {
//			String key = (String) iterator.next();
//			result.add(find(bucket,key));
//		}
//		return result;}	
	
	@Override
	public String toString() {
		return "MusicBand [description=" + description + ", name=" + name + "]";
	}		
}
