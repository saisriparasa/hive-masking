package com.amazonaws.proserv.masking_hive;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Hive Data Masking
 *
 */
public final class Md5Mask extends UDF 
{	
    public Text evaluate(final Text s) throws NoSuchAlgorithmException
    {
        if (s == null){
        	return null;
        }
        
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(s.toString().getBytes());
        byte[] hash = md.digest();
        StringBuilder str = new StringBuilder();
        for (byte b: hash){
        	str.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        }
        
        return new Text(str.toString()); 
    }
}
