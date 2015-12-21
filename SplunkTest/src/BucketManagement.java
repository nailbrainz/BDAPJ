import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class BucketManagement<E> {
	final int bucketSize;
	final int basicBucketSize = 1000000;
	
	final String[] bucketTypes = {"Hot", "Warm", "Cold"};
	final int[] maxBucketNum = new int[bucketTypes.length];
	final double fpProb;
	final String PATH;
	long id = 0l;
	boolean D = false;
	
	
	Queue<BloomFilter<E>> hotBuckets = new LinkedList<BloomFilter<E>>();
	Queue<BloomFilter<E>> warmBuckets = new LinkedList<BloomFilter<E>>();
	Queue<BloomFilter<E>> coldBuckets = new LinkedList<BloomFilter<E>>();
	
	
	
	void CheckWarmBucket(){
		if(warmBuckets.size() > maxBucketNum[1]){
			int convertingNum = warmBuckets.size()/10;
			for(int i = 0; i<convertingNum; i++){
				coldBuckets.add(warmBuckets.poll());
			}
			if(D)System.out.println( "warm bucket full, converting oldest 10% warm to cold");
			CheckColdBucket();
		}
	}
	void CheckColdBucket(){
		if(coldBuckets.size() > maxBucketNum[2]){
			int convertingNum = coldBuckets.size()/10;
			for(int i = 0; i<convertingNum; i++){
				coldBuckets.poll();
			}
			if(D)System.out.println( "cold bucket full, Removing oldest 10% cold buckets");
		}
	}
	void add(E data){
		id = (id+1)%(Long.MAX_VALUE-1);
		if(hotBuckets.size() == 0){
			hotBuckets.add(new BloomFilter<E>(fpProb, bucketSize, id));
		}
		BloomFilter front = hotBuckets.peek();
		front.add(data);
		if(hotBuckets.size() < maxBucketNum[0]){
			if(front.count() < front.getExpectedNumberOfElements()){
				if(D)System.out.println( "Successfully added to Bloom filter");
			}else{
				hotBuckets.add(new BloomFilter<E>(fpProb, bucketSize, id));
				if(D)System.out.println( "Created new Hot bucket, and added data to Bloom filter");
			}
		}else{
			int convertingNum = hotBuckets.size()/10;
			for(int i = 0; i<convertingNum; i++){
				warmBuckets.add(hotBuckets.poll());
			}
			CheckWarmBucket();
			//if(D)System.out.println( "Hot bucket full, converting oldest 10% hot to warm");
		}
	}
	
	public BucketManagement(String indexName, double fpProb){
		this.fpProb = fpProb;
		PATH = "D:/Gangplunk/"+indexName+"/";
		createFolder();
		Document doc = null;
		try {
			File inputFile = new File(PATH+indexName+".xml");
	        DocumentBuilderFactory dbFactory 
	           = DocumentBuilderFactory.newInstance();
	        DocumentBuilder dBuilder;
			dBuilder = dbFactory.newDocumentBuilder();
			 doc = dBuilder.parse(inputFile);
	        doc.getDocumentElement().normalize();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		NodeList bslist = doc.getElementsByTagName("BucketSize");
		Node bsnd = bslist.item(0);
		if(bsnd.getNodeType() == Node.ELEMENT_NODE){
			 Element finalOne = (Element)bsnd;
			 bucketSize = Integer.parseInt(finalOne.getAttribute("value"));
			 System.out.println(bucketSize);
		}else{
			bucketSize = basicBucketSize;
		}
		for(int i = 0; i<3; i++){
			NodeList list = doc.getElementsByTagName(bucketTypes[i]);
	        for(int j = 0; j<list.getLength(); j++){
	        	Node nd = list.item(j);
	        	if(nd.getNodeType() == Node.ELEMENT_NODE){
	        		 Element eElement = (Element) nd;
	        		 NodeList tmpList = eElement.getElementsByTagName("MaxBucketNumber");
	        		 Node ndd = tmpList.item(0);
	        		 if(ndd.getNodeType() == Node.ELEMENT_NODE){
	        			 Element finalOne = (Element)ndd;
	        			 maxBucketNum[i] = Integer.parseInt(finalOne.getAttribute("value"));
	        			 System.out.println(maxBucketNum[i]);
	        		 }
	        	}
	        }
		}
		
		
		//TODO : push here reading code
		Scanner scan = null;
		try {
			scan = new Scanner(new File(PATH+"bucketList.txt")).useDelimiter("\t|\r\n");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i = 0; i<bucketTypes.length && scan.hasNext(); i++){
			String type = scan.next();
			int num = scan.nextInt();
			for(int j = 0; j<num; j++){
				String bucketID = scan.next();
				FileInputStream fis;
				try {
					fis = new FileInputStream(PATH + "buckets/"+bucketID);
					ObjectInputStream ois = new ObjectInputStream(fis);
					BloomFilter bm = (BloomFilter) ois.readObject();
					id = Math.max(id, bm.getID());
					switch(i){
						case 0 : hotBuckets.add(bm); break;
						case 1 : warmBuckets.add(bm); break;
						case 2 : coldBuckets.add(bm); break;
					}
					ois.close();
					fis.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		System.out.println("ID = " + id);
		
	}
	private void createFolder(){
		
		try{
			new File(PATH).mkdir();
			new File(PATH+"buckets").mkdir();
	    } 
	    catch(SecurityException se){
	        //handle it
	    }
	}
	
	private int getBucketType(String str){
		switch(str){
			case "Hot":
				return 0;
			case "Warm":
				return 1;
			case "Cold":
				return 2;
		}
		return 0;
	}
	private String getBucketType(int integ){
		return bucketTypes[integ];
	}
	
	public void close() throws Exception{
		//TODO : serializable writing code
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(PATH+"bucketList.txt"), false));
		for(int j = 0; j<bucketTypes.length; j++){
			Queue<BloomFilter<E>> q = null;
			if(j == 0){
				q = hotBuckets;
			}else if(j == 1)q = warmBuckets;
			else if(j == 2)q = coldBuckets;
			
			bw.write(bucketTypes[j]+"\t" + q.size());
			bw.newLine();
			while(!q.isEmpty()){
				String path = PATH+"buckets/";
				BloomFilter bf = q.poll();
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path+bf.getID()));
				oos.writeObject((Object)bf);
			    oos.close();
			    bw.write(bf.getID()+"");
			    bw.newLine();
			}
		}
		
		bw.close();
	}
	
	void print(){
		System.out.println("bucket item size = " + bucketSize);
		for(int i = 0; i<bucketTypes.length; i++){
			System.out.println(bucketTypes[i] + " bucket  size = " + maxBucketNum[i]);
		}
		System.out.println("current Hot size = " + hotBuckets.size());
		System.out.println("current Warm size = " + warmBuckets.size());
		System.out.println("current Cold size = " + coldBuckets.size());
		
	}
}
