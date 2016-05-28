package com.amazonaws.proserv.masking_hive;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Simple Hive Data Masking
 *
 */
public final class s_SHA1Mask extends UDF 
{	
    public Text evaluate(final Text s) throws NoSuchAlgorithmException
    {
        if (s == null){
        	return null;
        }      
        
        MessageDigest md = MessageDigest.getInstance("SHA1");
        byte[] hash = md.digest(s.toString().getBytes());        
        StringBuffer hashtext = new StringBuffer();
        
        for(int i = 0;i<hash.length; i++){
        	hashtext.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
        }

        return new Text(hashtext.toString());
    }
}
