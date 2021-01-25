package dont_panic;

import java.awt.Polygon;
import java.util.LinkedList;

public class RefOverFlowPage extends Ref {
	LinkedList<OverFlowPage> pointers;
	int Size;
public RefOverFlowPage(int n) {
	pointers=new LinkedList<OverFlowPage>();
	Size=n;
}
public LinkedList getPointers() {
	return pointers;
}

public void insertPointer(Ref r) {
	RefPage s=(RefPage) r;

	if(pointers.isEmpty()) {
		OverFlowPage<Ref> newOverFlowPage=new OverFlowPage<Ref>(Size);
		newOverFlowPage.add(r);
		pointers.add(newOverFlowPage);
		return;
	}
for (int i = 0; i < pointers.size(); i++) {
OverFlowPage page=(OverFlowPage) pointers.get(i);
if(!page.isFull()) {
	page.add(r);
	return;
}
}
OverFlowPage<Ref> newOverPage=new OverFlowPage<Ref>(Size);
newOverPage.add(r);
pointers.add(newOverPage);
return;
}

public boolean isFull() {
	if(pointers.size()>=Size) {
		return true;
	}
	return false;
}
public int maxReference() {
	int max=0;
	System.out.println("size"+pointers.size());
for (int i = 0; i < pointers.size(); i++) {
	OverFlowPage page=(OverFlowPage) pointers.get(i);
	for (int j = 0; j <page.size(); j++) {
		RefPage reference=(RefPage) page.get(j);
		System.out.println("right");
			if(max<reference.getPage()) {
				System.out.println("wrong");
				max=reference.getPage();
			
		}
	}
}	

return max;
}
public boolean deleteDuplicate(int page) {
	for (int i = 0; i <pointers.size(); i++) {
		OverFlowPage<RefPage> overflow=pointers.get(i);
		for (int j = 0; j < overflow.size(); j++) {
			if(overflow.get(j).getPage()==page) {
				overflow.remove(j);
				if(overflow.isEmpty()) {
						System.out.println("page deleted"+page);
					pointers.remove(i);
				}
				i=pointers.size();
				break;
			}
		}
	}
	System.out.println("page:"+page);
	System.out.println(pointers.size());
	System.out.println(pointers.isEmpty());
	return pointers.isEmpty();
	
}
public void minusPage(int deletedpage) {
	for (int i = 0; i < pointers.size(); i++) {
		OverFlowPage page=(OverFlowPage) pointers.get(i);
		for (int j = 0; j <page.size(); j++) {
			RefPage f=(RefPage) page.get(j);
		      if(f.getPage()>deletedpage) {
		    	  f.minusPage(deletedpage);
		      }
		}
		
		}
}
public RefOverFlowPage polygonEqual(Polygon a) {
	RefOverFlowPage result=new RefOverFlowPage(Size);
	for (int i = 0; i < pointers.size(); i++) {
		OverFlowPage page=pointers.get(i);
		
		for (int j = 0; j < page.size(); j++) {
			RefPage ref=(RefPage) page.get(j);
			if(DBApp.equals((Polygon) ref.key, a)) {
				result.insertPointer(ref);
			}
		}
	}
	return result;
}

public void getInfo() {
	int count=0;
for (int i = 0; i < pointers.size(); i++) {
	OverFlowPage page=pointers.get(i);
	for (int j = 0; j <page.size(); j++) {
		count=count+1;
	   RefPage ref=(RefPage) page.get(j);
	   System.out.println("page:"+ref.getPage());
	}
}
System.out.println("size:"+count);

}

}