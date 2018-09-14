package com.share.util;

public class StringUtil {
	public static boolean isEmpty(String target){
		if(target==null||"".equals(target)||"null".equals(target)){
			return true;
		}
		return false;
	}
}
