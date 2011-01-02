package play.modules.riak;

/**
 * Encapsulate url information
 */
public class RiakKey {

	private String bucket;
	private String key;
	
	public RiakKey(String bucket, String key) {
		super();
		this.bucket = bucket;
		this.key = key;
	}	
	
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "RiakKey [bucket=" + bucket + ", key=" + key + "]";
	}
	
}
