package cassandra;

import java.util.Random;


public class Randips 
{
        
    public String ipsCreation(int i)
    {    
        String ips; 
        Random ip = new Random();
        Random port = new Random();
            
        ips = ip.nextInt(256) + "." + ip.nextInt(256) + "." + ip.nextInt(256) + "." + ip.nextInt(256) + " " + ":" + " " + port.nextInt(8000);
        System.out.println(ips); 
        
        return ips;
    }
    
}
