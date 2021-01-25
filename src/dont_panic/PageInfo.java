package dont_panic;

import java.io.File;
import java.io.Serializable;

public class PageInfo implements Serializable {
	private Object max;
	private Object min;
	private File data;
	private boolean lastPage;
	public boolean isfull=false;
	public PageInfo(Object max,Object min,File data) {
		this.max=max;
		this.min=min;
		this.data=data;
		lastPage=true;
	}
	public boolean isLastPage() {
		return lastPage;
	}
	public void setLastPage(boolean lastPage) {
		this.lastPage = lastPage;
	}
	public Object getMax() {
		return max;
	}
	public void setMax(Object max) {
		this.max = max;
	}
	public Object getMin() {
		return min;
	}
	public void setMin(Object min) {
		this.min = min;
	}
	public File getData() {
		return data;
	}
	public void setData(File data) {
		this.data = data;
	}
	public boolean isFull() {
		return isfull;
	}
	public void setfull(boolean a) {
		isfull=a;
	}
	

}
