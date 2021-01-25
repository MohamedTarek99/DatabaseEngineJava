package dont_panic;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page<T> extends Vector<T>  {
      
	public static int count=0;
	public int size;
	File file;
	String key;
	public Page(String key) throws IOException {
		super();
		FileReader reader=new FileReader("config//DBApp.properties");  
	      
	    Properties p=new Properties();  
	    p.load(reader); 
	    size=Integer.parseInt((String) p.get("MaximumRowsCountinPage"));
		this.key=key;
		 file=new File("data\\"+count+".class"); //bn3mel create lel file 3shan n7gez path bas law mawgood bn3mel
		 while(file.exists()) {                    //iterate leghayet mnla2y wa7ed mesh mawgood 3shan man3mlesh
		count=count+1;                              //override le file bta3 page tany
		 file=new File("data\\"+count+".class");     
		 }
		 
		 
	}
	
	public Object getmin() {
		Hashtable temp=(Hashtable) get(0);
		return temp.get(key)
		
		;
	}
	
	public Object getmax() {
		Hashtable temp=(Hashtable) get(elementCount-1);
		return temp.get(key);
		}
	

	public File save() throws IOException {
		FileOutputStream f = new FileOutputStream(file);
		ObjectOutputStream o = new ObjectOutputStream(f); //hena ba3mel save lel file
		
			
				 o.writeObject(this);
				    o.close();
				    f.close();
				    return file;
			
	}
	
	public boolean isFull() {
		if(elementCount>=size) {
			return true;                    //hena bt2aked en el elements el gowa el page ma3detsh el limit
		}else {
			return false;
		}
		
	}
	
	
}
