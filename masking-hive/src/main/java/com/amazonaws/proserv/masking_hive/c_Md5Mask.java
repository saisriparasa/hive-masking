package com.amazonaws.proserv.masking_hive;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(
        name = "c_md5mask",
        value = "_FUNC_(page) - returns the masked value for the argument",
        extended = "")
public final class c_Md5Mask extends GenericUDF {
    private final Text result = new Text();
    private ObjectInspector argumentOI;

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		
		if(args.length != 1){
			throw new UDFArgumentLengthException("Expected length is 1");
		}
		
		ObjectInspector data = args[0];
		
		PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) data).getPrimitiveCategory();
        
        if(primitiveCategory != PrimitiveCategory.STRING){
        	throw new UDFArgumentTypeException(0,"A String type expected, but "+data.getTypeName()+" has been provided")	;
        }
        
		argumentOI = data;
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;

	}
	
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		
		if (args[0] == null){
        	System.out.println("haha got you");
        	return null;
        }  
		
		result.clear();
		
		if (args.length ==1){
			String data = ((StringObjectInspector) argumentOI).getPrimitiveJavaObject(args[0].get());
			
	        MessageDigest md = null;
	        
			try {
				md = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	        byte[] hash = md.digest(data.toString().getBytes());
	        BigInteger b = new BigInteger(1, hash);
	        String hashtext = b.toString(16);
	        
	        //Prepend zero if not 32 char
	        while (hashtext.length() < 32){
	        	hashtext = "0" + hashtext;
	        }
	        
	        result.set(hashtext.toString());
		}
		
		return result;
	}

	@Override
	public String getDisplayString(String[] args) {
		return "c_md5mask(" + args[0] + ")";
	}


}
