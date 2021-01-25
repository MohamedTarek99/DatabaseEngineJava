package dont_panic;

import java.awt.Polygon;
import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Scanner;

public class loli {

	
	public static void main(String [] args) {
		 int[] p3x = {400, 400, 460, 460, 440, 440, 420, 420, 400};
		 int[] p3y = {400, 460, 460, 400, 400, 440, 440, 400, 400};
		 Polygon p1 = new Polygon(p3x, p3y, p3x.length);
        Polygon p2=new Polygon(p3x, p3y, p3x.length); 	
		System.out.println(p1.equals(p2));
		
		
}

}