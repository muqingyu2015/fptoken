package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import org.apache.lucene.util.BytesRef;

public class Utils {
	public static String BytesReftoString(BytesRef ref)
	{
		if(ref==null)
		{
			return "null";
		}
		if(ref.length<=0)
		{
			return "";
		}
		try {
			return ref.utf8ToString();
		}catch(Throwable e)
		{
			return ref.toHexString();
		}
	}
}
