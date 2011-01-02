package play.modules.riak;

import java.lang.reflect.Type;
import java.util.List;

import org.json.JSONException;

import com.basho.riak.client.mapreduce.JavascriptFunction;

import javassist.CtClass;
import javassist.CtMethod;
import play.Logger;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.classloading.enhancers.Enhancer;

public class RiakEnhancer extends Enhancer {

	public static final String PACKAGE_NAME = "play.modules.riak";

	public static final String ENTITY_ANNOTATION_NAME = "play.modules.riak.RiakEntity";
	public static final String ENTITY_ANNOTATION_VALUE = "value";

	@Override
	public void enhanceThisClass(ApplicationClass applicationClass) throws Exception {

		final CtClass ctClass = makeClass(applicationClass);

		// Enhance RiakEntity annotated classes
		if (hasAnnotation(ctClass, ENTITY_ANNOTATION_NAME)) {
			enhanceRiakEntity(ctClass, applicationClass);
		}
		else {
			return;
		}
	}

	/**
	 * Enhance classes marked with the RiakEntity annotation.
	 * 
	 * @param ctClass
	 * @throws Exception
	 */
	private void enhanceRiakEntity(CtClass ctClass, ApplicationClass applicationClass) throws Exception {
		// Don't need to fully qualify types when compiling methods below
		classPool.importPackage(PACKAGE_NAME);
		String entityName = ctClass.getName();
		Class clazz = this.getClass();
		
		// - Implement methods
		Logger.debug( clazz.getName() + "-->enhancing RiakEntity-->" + ctClass.getName());

		// /!\WARNING/!\ Generics won't works in javassist		
		// find 		
		CtMethod find = CtMethod.make("public static RiakModel find(String bucket, String key){" +
				"com.basho.riak.client.response.FetchResponse r = RiakPlugin.riak.fetch(bucket, key);" +
				"if(r.hasObject()){" +
					"com.basho.riak.client.RiakObject o = r.getObject();" +
					entityName + " e = (" + entityName + ")new com.google.gson.Gson().fromJson(o.getValue(), " + entityName + ".class);" +
					"e.setObj(o);"+
					"return e;" +
				"}" +
				"return null;}", ctClass);
		ctClass.addMethod(find);		

		CtMethod find2 = CtMethod.make("public static RiakModel find(Class clazz, String key){" +
				"return find(RiakPlugin.getBucketName(clazz), key); }",ctClass);
		ctClass.addMethod(find2);
		
		CtMethod findAll = CtMethod.make("public static java.util.List findAll(String bucket){" +
				"java.util.Collection keys = "+ entityName + ".findKeys(bucket);" +
				"java.util.List result = new java.util.ArrayList();"+
				"for (java.util.Iterator iterator = keys.iterator(); iterator.hasNext();) {"+
					"String key = (String) iterator.next();"+
					"result.add(find(bucket,key));"+
				"}"+
				"return result;}", ctClass);
		
		ctClass.addMethod(findAll);
		
		
		CtMethod findAll2 = CtMethod.make("public static java.util.List findAll(Class clazz){" +
				"return findAll(RiakPlugin.getBucketName(clazz));}",ctClass);
		ctClass.addMethod(findAll2);
		
		
		CtMethod fetch = CtMethod.make("public static java.util.List fetch(Class clazz, java.lang.reflect.Type returnType, int start, int end){"+
		"try {"+
			"int[] array = new int[2];"+
			"if(start != -1 && end != -1){"+
				"array[0] = start;"+
				"array[1] = end;"+
			"}"+
			"com.basho.riak.client.response.MapReduceResponse r = play.modules.riak.RiakPlugin.riak.mapReduceOverBucket(RiakPlugin.getBucketName(clazz))"+
				".map(com.basho.riak.client.mapreduce.JavascriptFunction.named(\"Riak.mapValuesJson\"), false)"+
				".reduce(com.basho.riak.client.mapreduce.JavascriptFunction.named(\"Riak.reduceSlice\"),array,true).submit();"+
		    "if (r.isSuccess()) {"+
		    	"java.util.List jsonResult = new com.google.gson.Gson().fromJson(r.getBodyAsString(), returnType);"+
		    	"return jsonResult;"+
		    "}else{"+
		    	"System.out.println(\"Error during fetch for class \");"+
		    "}"+	
		"} catch (org.json.JSONException e) {"+
			"e.printStackTrace();"+
		"}"+		
		"return null;}"		
		,ctClass);
		ctClass.addMethod(fetch);
		
		// Done.
		applicationClass.enhancedByteCode = ctClass.toBytecode(); 
		ctClass.detach();
	}
}
