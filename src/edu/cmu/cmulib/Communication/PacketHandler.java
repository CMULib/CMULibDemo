package edu.cmu.cmulib.Communication;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class PacketHandler {
	@SuppressWarnings("rawtypes")
	ConcurrentHashMap<Class<?>, ConcurrentLinkedDeque> map = new ConcurrentHashMap<>();
	
	public void register(Class<?> clazz, ConcurrentLinkedDeque queue){
		map.put(clazz, queue);
    }
	@SuppressWarnings("unchecked")
	public void handlePacket(Object obj){
		if(map.containsKey(obj.getClass())){
			
            //synchronized(map.get(obj.getClass())){
			    map.get(obj.getClass()).add(obj);
			   // System.out.println((Double)obj);
		   // }
		}
    }
	
	/*public static void main(String[] args){
		LinkedList<String> sList = new LinkedList<String>();
		LinkedList<Double> dList = new LinkedList<Double>();
		PacketHandler p = new PacketHandler();
		p.register(String.class,sList );
		p.register(Double.class,dList );
		p.handlePacket("abcde");
		p.handlePacket(1.25);
		System.out.println(sList.get(0));
		System.out.println(dList.get(0));
		
		
	}*/
	
	
}
