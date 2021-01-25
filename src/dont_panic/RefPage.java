package dont_panic;

public class RefPage extends Ref {
	private int page;
public RefPage(int page,Object key) {
	
	this.page=page;
	this.key=key;
}
public int getPage() {
	return page;
}
public void shiftPage() {
	this.page = page+1;
}
public void minusPage(int deletedpage) {
	if(page>deletedpage){
	page=page-1;

}
}
}