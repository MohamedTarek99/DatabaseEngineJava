package dont_panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements Serializable {
private String name;
private Vector<PageInfo> pages;
private String key;
private ArrayList<String> indexedColoumns;

public Table(String name,String key) {
this.name=name;	
this.key=key;
pages=new Vector<PageInfo>();
indexedColoumns=new ArrayList<String>();
}

public PageInfo getInfoByPath(File path) {
	for (int i = 0; i < pages.size(); i++) {
	     if(pages.get(i).getData().equals(path)) {
	    	 return pages.get(i);
	     }
	}
	return null;
	
}
public void addIndex(String coloumn) {
	indexedColoumns.add(coloumn);
}
public ArrayList<String> getIndexes(){
	return indexedColoumns;
}

public String getName() {
	return name;
}


public String getKey() {
	return key;
}
public void checklast() {
	if(pages.size()!=0) {
		
	pages.get(pages.size()-1).setLastPage(true);
}
}


public void insertPage(PageInfo info) {
pages.add(info);
if(pages.size()!=1) {
	PageInfo prev_page=pages.get(pages.size()-2);
    prev_page.setLastPage(false);
}
}

public Vector<PageInfo> getPages(){
	                                              // bageeb el pages bta3t el table 23raf awsal lel tuples
	return pages;
}

public void save() throws IOException {
	File file=new File("data\\"+name+".class");
	FileOutputStream f = new FileOutputStream(file);        //ba3mel save lel table
	ObjectOutputStream o = new ObjectOutputStream(f);

		
			 o.writeObject(this);
			    o.close();
			    f.close();
	
}
}
