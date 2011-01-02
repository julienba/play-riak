package models.riak;

import play.modules.riak.RiakEntity;
import play.modules.riak.RiakModel;

@RiakEntity(key="year")
public class Year extends RiakModel{
	
	public String year;

	public Year(String year) {
		super();
		this.year = year;
	}
}
