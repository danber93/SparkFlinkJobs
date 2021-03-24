import java.util.*;

import scala.Tuple5;  

class MyComparator implements Comparator<Tuple5<String,Long,Double, Double, String>>{  
	
	public int compare(Tuple5<String,Long,Double, Double, String> o1,Tuple5<String,Long,Double, Double, String> o2){  
		Tuple5<String,Long,Double, Double, String> s1=o1;  
		Tuple5<String,Long,Double, Double, String> s2=o2;  

		return s1._2().compareTo(s2._2());  
	}  
}  