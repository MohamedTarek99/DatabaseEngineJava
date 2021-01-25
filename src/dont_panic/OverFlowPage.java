package dont_panic;

import java.io.Serializable;
import java.util.Vector;

public class OverFlowPage<T> extends Vector<T> {
	int Size;
public OverFlowPage(int n){
	super();
	Size=n;
}
public boolean isFull() {
	
	return this.size()>=Size;
	
}
}
