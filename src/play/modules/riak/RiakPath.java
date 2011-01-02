package play.modules.riak;

public class RiakPath {

	private String value;
	private RiakKey riakKey;
		
	public RiakPath(String value, RiakKey riakKey) {
		super();
		this.value = value;
		this.riakKey = riakKey;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public RiakKey getRiakKey() {
		return riakKey;
	}

	public void setKey(RiakKey riakKey) {
		this.riakKey = riakKey;
	}

	public String getBucket(){
		return this.riakKey.getBucket();
	}
	
	public String getKey(){
		return this.riakKey.getKey();
	}	
	
	@Override
	public String toString() {
		return "RiakPath [riakKey=" + riakKey + ", value=" + value + "]";
	}
}
