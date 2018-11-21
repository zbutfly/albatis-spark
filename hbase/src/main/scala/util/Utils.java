package util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;


/**
 * 通用工具类
 */
public class Utils {
	
	/**
	 * 判断是否为空，支持字符串、map、集合、数组和其它对象
	 * 
	 * @param obj				判断的对象
	 * @return 					为空返回True，不为空返回False
	 */
	public static boolean isEmpty(Object obj){
		if(obj == null){
			return true;
		}else if (obj instanceof String){
			return "".equals(String.valueOf(obj).trim());
		}else if (obj instanceof Map<?,?>){
			return ((Map<?,?>) obj).isEmpty();
		}else if (obj instanceof Collection<?>){
			return ((Collection<?>) obj).isEmpty();
		}else if (obj.getClass().isArray()){
			return Array.getLength(obj) == 0;
		}
		return false;
	}
	
	/**
	 * 判断是否为空，支持字符串、map、集合、数组和其它对象
	 * 
	 * @param obj				判断的对象
	 * @return 					为空返回False，不为空返回True
	 */
	public static boolean isNotEmpty(Object obj){
		return !isEmpty(obj);
	}
	

}

