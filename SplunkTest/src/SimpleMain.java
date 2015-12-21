import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimpleMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		BucketManagement bmm = new BucketManagement("urlData", 0.001);
        Random r = new Random(5);
        
        
        int elementCount = 100000;
        for (int i = 0; i < elementCount; i++) {
            byte[] b = new byte[200];
            r.nextBytes(b);
            bmm.add(new String(b));
        }
        
        bmm.print();
        bmm.close();
        
        BucketManagement bm2 = new BucketManagement("urlData", 0.001);
        System.out.println("From File!");
        bm2.print();
        
        BloomFilter<String> bf = new BloomFilter<String>(0.001, elementCount, 0);
		System.out.println("THis is " + bf.getExpectedNumberOfElements());
        for (int i = 0; i < elementCount; i++) {
            byte[] b = new byte[200];
            r.nextBytes(b);
            bf.add(new String(b));
        }
        
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("a"));
        oos.writeObject((Object)bf);
        oos.close();
        
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("a"));
		BloomFilter bm = (BloomFilter) ois.readObject();
		ois.close();
        System.out.println("in = " + bm.getExpectedNumberOfElements());/**/
	}
	
}
