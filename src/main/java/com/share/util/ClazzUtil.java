package com.share.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class ClazzUtil {

	public Map<String,Object> reflect(Object obj){
		Map<String,Object> map = new LinkedHashMap<String, Object>();
		Class<? extends Object> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			String name = field.getName();
			if(name.equalsIgnoreCase("serialVersionUID")||name.contains("$this")){
				continue;
			}
			String type = field.getType().getSimpleName();
			try {
				Method method = clazz.getMethod((type.equalsIgnoreCase("boolean")?"is":"get")+name.substring(0, 1).toUpperCase()+ name.substring(1));
				Object value = method.invoke(obj);
				if(type.equalsIgnoreCase("String")||type.equalsIgnoreCase("Date")){
					if(type.equalsIgnoreCase("Date")){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					value = value.toString();
				}
				map.put(name, value);
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		return map;
	}
}
