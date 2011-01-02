package models.riak;

import play.modules.riak.RiakEntity;
import play.modules.riak.RiakModel;

@RiakEntity(key="name",bucket="AlbumTest")
public class Album extends RiakModel{
	
	public String name;
	public int year;
	
	public Album(String name, int year) {
		super();
		this.name = name;
		this.year = year;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	@Override
	public String toString() {
		return "Album [name=" + name + ", year=" + year + "]";
	}
}
