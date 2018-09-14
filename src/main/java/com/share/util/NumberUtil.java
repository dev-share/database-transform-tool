package com.share.util;

public class NumberUtil {
	public static boolean isNumber(String target){
		if(target==null||"".equals(target)||"null".equals(target)){
			return false;
		}
		for(int i=0;i<target.length();i++){
			char c = target.charAt(i);
			if(!(c>='0'&&c<='9')){
				return false;
			}
		}
		return true;
	}
}
