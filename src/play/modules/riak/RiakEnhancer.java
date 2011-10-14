package play.modules.riak;

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

		// /!\WARNING/!\ Generics won't works in javassist, 
		// hard to debug, 
		// TIPS: write method with absolute object path in class implement RiakModel and copy paste, be patient
		 				
		CtMethod find = CtMethod.make("public static RiakModel find(String bucket, String key){" +
			"try {" +
				"com.basho.riak.client.IRiakObject ro = play.modules.riak.RiakPlugin.riak.fetchBucket(bucket).execute().fetch(key).execute();" +
				"if(ro != null){" +
					entityName +" e = (" + entityName + ")new com.google.gson.Gson().fromJson(ro.getValueAsString(), " +
					entityName +".class);" +
					"e.setObj(ro);" +
					"return e;" +
				"}" +
			"} catch(com.basho.riak.client.RiakException e1){" +
				"e1.printStackTrace();" +
			"}"+
			"return null;}", ctClass);
		ctClass.addMethod(find);		
			

		CtMethod find2 = CtMethod.make("public static RiakModel find(Class clazz, String key){" +
				"return find(play.modules.riak.RiakPlugin.getBucketName(clazz), key); }",ctClass);
		ctClass.addMethod(find2);
		
		CtMethod findAll = CtMethod.make("public static java.util.List findAll(String bucket){" +
				"java.lang.Iterable keys = " + entityName + ".findKeys(bucket);" +
				"java.util.List result = new java.util.ArrayList();"+
				"for (java.util.Iterator iterator = keys.iterator(); iterator.hasNext();) {"+
					"String key = (String) iterator.next();"+
					"result.add(find(bucket,key));"+
				"}"+
				"return result;}", ctClass);
		
		ctClass.addMethod(findAll);
		
		
		CtMethod findAll2 = CtMethod.make("public static java.util.List findAll(Class clazz){" +
				"return findAll(play.modules.riak.RiakPlugin.getBucketName(clazz));}", ctClass);
		ctClass.addMethod(findAll2);

		
		CtMethod fetch = CtMethod.make("public static java.util.List fetch(Class clazz, java.lang.reflect.Type returnType, int start, int end){"+
			"int[] array = new int[2];"+
			"if(start != -1 && end != -1){"+
				"array[0] = start;"+
				"array[1] = end;"+
			"}"+
            "com.basho.riak.client.query.MapReduce mr = play.modules.riak.RiakPlugin.riak.mapReduce(play.modules.riak.RiakPlugin.getBucketName(clazz))"+
			    ".addMapPhase(new com.basho.riak.client.query.functions.NamedJSFunction(\"Riak.mapValuesJson\"), false)"+
			    ".addReducePhase(new com.basho.riak.client.query.functions.NamedJSFunction(\"Riak.reduceSlice\"), true);"+
			"try {"+
				"com.basho.riak.client.query.MapReduceResult mrs = mr.execute();"+
                "String res = mrs.getResultRaw();"+
                "if(!res.isEmpty()){"+
                    "java.util.List jsonResult = new com.google.gson.Gson().fromJson(res, returnType);"+
                    "return jsonResult;"+				
				"}"+			
			"}catch (com.basho.riak.client.RiakException e) {"+
				"e.printStackTrace();"+
			"}"+
			"return null;}",ctClass);
		ctClass.addMethod(fetch);
		
		// Done.
		applicationClass.enhancedByteCode = ctClass.toBytecode(); 
		ctClass.detach();
	}
}
