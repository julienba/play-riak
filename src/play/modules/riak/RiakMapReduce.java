package play.modules.riak;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.basho.riak.pbc.MapReduceResponseSource;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.mapreduce.JavascriptFunction;
import com.basho.riak.pbc.mapreduce.MapReduceBuilder;
import com.basho.riak.pbc.mapreduce.MapReduceResponse;
import com.google.protobuf.ByteString;

import play.Logger;
import play.Play;

public class RiakMapReduce {
	
	public static Map<String, String> function = new HashMap<String, String>();
	
	
	public static void loadQuery(){
		
		String rootPath = Play.modules.get("riak").getRealFile().getAbsolutePath() + "/src/play/modules/riak/mapreduce";
		Logger.debug("Load script in %s", rootPath);
		//load core directory (play.modules.riak.mapreduce)
		//TODO: and custom define in riak.mapreduce.input
		File dir = new File(rootPath);
		
		if(!dir.exists()){
			Logger.info("Dir play.modules.riak.mapreduce not exist" );
			return;
		}
		
		String[] scriptList = dir.list();
		
		for (String file : scriptList) {
			Logger.debug("Load script: %s", file);
			String content = "";
			String cleanFileName = "";
			
			if(file.endsWith(".js")){
				content = getJavascriptfile(rootPath + "/" + file);
				cleanFileName = file.substring(0, file.length() - ".js".length());
			}else if(file.endsWith(".coffee")){
				content = getCoffeeFile(rootPath + "/" + file);
				cleanFileName = file.substring(0, file.length() - ".coffee".length());
			}
			
			if(!content.isEmpty()){
				function.put(cleanFileName, content);
			}
		}
	}
	
	
	private static String getJavascriptfile(String filename){
		File file = new File(filename);
        String content;
		try {
			content = FileUtils.readFileToString(file);
			return content;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}		
	}
	
    private static String getCoffeeFile(String filename) {
        try {
            //File file = new File(filename + ".coffee");
        	File file = new File(filename);
            String content = FileUtils.readFileToString(file);
            return new org.jcoffeescript.JCoffeeScriptCompiler().compile(content);
        }
        catch (Exception e) {
        	e.printStackTrace();
            return "";
        }
    }

	public static long count(Class clazz){
		
		MapReduceBuilder builder = new MapReduceBuilder();
		builder.setBucket(RiakPlugin.getBucketName(clazz));
		builder.setRiakClient(RiakPlugin.riak);
		builder.map(JavascriptFunction.anon(RiakMapReduce.function.get("count")), false);
		builder.reduce(JavascriptFunction.named("Riak.reduceSum"),true);
		
		try {
			MapReduceResponseSource mrs = builder.submit(new RequestMeta().contentType("application/json"));
			while(mrs.hasNext()){
				MapReduceResponse mr = mrs.next();
				ByteString bs = mr.getContent();
				if(bs != null && !bs.isEmpty()){
					String resString = bs.toStringUtf8();
					resString = resString.substring(1, resString.length() -1);
					return Long.parseLong(resString);
				}
			}			
		}catch (IOException e) {
			e.printStackTrace();
		}		
		return -1;
	}
}
