package com.amazonaws.proserv.masking_hive;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Simple Hive Data Masking
 *
 */
public final class s_Md5Mask extends UDF 
{	
    public Text evaluate(final Text s) throws NoSuchAlgorithmException
    {
        if (s == null){
        	return null;
        }      
        
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hash = md.digest(s.toString().getBytes());
        BigInteger b = new BigInteger(1, hash);
        String hashtext = b.toString(16);
        
        //Prepend zero if not 32 char
        while (hashtext.length() < 32){
        	hashtext = "0" + hashtext;
        }
        
        return new Text(hashtext.toString());
    }
}
