package dont_panic;

import java.awt.Dimension;
import java.awt.Polygon;

public class poly {

	Polygon polygon;
	public poly(Polygon polygon) {
		this.polygon=polygon;
	}
	public int compareTo(Polygon pol) {
		Dimension dim=polygon.getBounds().getSize();
		int area=dim.height*dim.width;
		Dimension dim2=pol.getBounds().getSize();
		int area2=dim2.height*dim2.width;
		if(area>area2) {
			return 1;
		}
		if(area<area2) {
			return -1;
		}
		// TODO Auto-generated method stub
		return 0;
	}

	
	
	
}
