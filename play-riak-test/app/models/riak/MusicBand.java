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
		
	@Override
	public String toString() {
		return "MusicBand [description=" + description + ", name=" + name + "]";
	}		
}
