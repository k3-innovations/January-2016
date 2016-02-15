package Assignment;

public class ReverseString {
	
	public StringBuffer ReverseStg(String str){
		//using in-built class for string reverse..
		
		 StringBuffer buffer = new StringBuffer(str);
	       buffer.reverse();
		   return buffer;
	}
	public String ManualReverse(String str){
		//using a user-defined method for string reverse...
		String lcStr = new String(str);
		String lcStr1 ="";
		for (int cnt=lcStr.length()-1;cnt>=0;cnt--){
			lcStr1+=lcStr.charAt(cnt);
		}
		return lcStr1;
		
	}
}
