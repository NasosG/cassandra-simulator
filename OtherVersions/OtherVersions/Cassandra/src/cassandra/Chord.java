package cassandra;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;


public class Chord 
{    
    private static List<String> lines;
    private ArrayList<String> nodes = new ArrayList<>();
    private ArrayList<Long> hashes = new ArrayList<>();
    private Random rand; 
    private int m = Cassandra.getNumberOfIPS();
    private int number_of_chord_nodes; 
    private ArrayList<Chord_node> node_list;    
        
    public Chord()
    {
        number_of_chord_nodes = Cassandra.getNumberOfNodes();
        Randips ips = new Randips();
        rand = new Random();
        // This array list maintains the chord nodes in the network
        node_list = new ArrayList<>();
                
        
        for (int i=0; i < number_of_chord_nodes; i++) {  
            String a = ips.ipsCreation(Cassandra.getNumberOfIPS());
            nodes.add(a);
        }
        
        // Generate and add chord nodes as per the ip table list
        for (int i = 0; i < number_of_chord_nodes; i++) {
            
            // Get the hash key of the ip address
            long nodes_index = getHash(nodes.get(i));
            
            // Add the chord node to the array list of chord nodes 
            node_list.add(new Chord_node(nodes_index));   
            
        }  
        
        // We sort the list of chord nodes according to their node indexes
        // so that they can occupy the chord and we can later find their 
        // predecessors and successors inside the chord
        Collections.sort(node_list, new Chord_node_comparator());
        
        // show nodes for debugging
        //for(Chord_node nd: node_list) {
        //    System.out.println("Κόμβος: " + nd.node_name);
        //}
        
        
        // we generate the successor and predecessor information for each node
        for (int i = 0; i < node_list.size(); i++) {
            Chord_node chordNode = node_list.get(i);
            if (i == node_list.size() - 1) {
                // In case of last node, first node of the list is our successor
                // and the previous node is our predecessor
                chordNode.predecessor = node_list.get(i-1);
                chordNode.successor = node_list.get(0);
            }
            else if (i == 0) {
                // In case of first node, last node of the list is our previous node 
                // and second node of the list is the next node in the chord
                chordNode.predecessor = node_list.get(node_list.size() - 1);
                chordNode.successor = node_list.get(1);
            }                
            else {
                // In case we are anywhere in between, we use our previous and next nodes
                // as predecessor and successor
                chordNode.predecessor = node_list.get(i-1);
                chordNode.successor = node_list.get(i+1);
            }
        }
        // We construct the finger tables for each chord node with this function
        fingerTablesCreation();
        // and give them their data and keys that they're responsible for
        storeDataToNodes();
    }
    
    // our hash function
    // we thought we should return unsigned int or long
    // in order to be safe in very big numbers
    // but we don't know if long is big enough
    // it wasn't a major problem
    public long getHash(String s)  
    {
        long num = 0;
        try {
            byte[] bytesOfMessage = s.getBytes("UTF-8");
            //do {
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            md.reset();
            md.update(s.getBytes("UTF-8")); 
            

                byte thedigest[] = md.digest(bytesOfMessage);

                ByteBuffer wrapped = ByteBuffer.wrap(thedigest); 
                num = wrapped.getLong();
                
               // we use absolute value and typecast in case of overflow and negative numbers
               num = Math.abs( num % (long) Math.pow(2, m));
               //System.out.println(num);
            //} while((hashes.contains(num)));
            
           hashes.add(num);
           //System.out.println("Κόμβος N" + num);
        }
        catch(Exception e) {
           System.err.println(e);
        }
         
        return num;
          
    }
    
    private void fingerTablesCreation()
    {
        long temp_index;
        // nodes that each node knows
        int no_of_identifier_bits = m;
        Chord_node node, temp_chord_node, last_chord_node;    
        
        last_chord_node = node_list.get(node_list.size() - 1);
        
        // for each chord node in the network
        for (int i = 0; i < number_of_chord_nodes; i++) {
            // get node's index
            node = node_list.get(i);
            
            // finger table will have as many entries as identifier bits
            for (int j = 0; j < no_of_identifier_bits; j++) {
                // The index of next node in the finger table is calculated as current node index + (2^j)
                temp_index = node.node_index + (1 << j);
                
                // if the calculated successor's index above is greater
                // than the number of keys (2^m - 1), we use modulus in order to retain
                // this circular structure
                if (temp_index >= last_chord_node.node_index)
                    temp_index %= Math.pow(2, node.m);
                
                if (temp_index < node.successor.node_index)
                    // If less than the successor node index add to the finger table
                    node.finger_table.add(node.successor);
                else {
                    // If the target calculated is greater than the successor node 
                    // index, then we iterate through the successor nodes one by
                    // one till we find the node greated than the calculated target
                    if (temp_index > last_chord_node.node_index) {
                        // If the target calculated happens to be greater than last
                        // chord node index but less than (2^m), we just use the 
                        // successor of the last chord node which will be the first
                        // node in the list of chord nodes
                        temp_chord_node = last_chord_node.successor;
                    }
                    else {
                        // Iterate through the list of chord nodes, using "successor"
                        // till we find node greater than the calculated target
                        temp_chord_node = node.successor;
                        while (temp_index > temp_chord_node.node_index) {

                            temp_chord_node = temp_chord_node.successor; 
                            
                        }
                    }
                    // Add the chord node to the finger table found from the above
                    node.finger_table.add(temp_chord_node);
                }
            }              
        }
    }
    
    private void storeDataToNodes()
    { 
        Map<Long,String> keysNvalue = new LinkedHashMap<>();
        File file = new File("src/cassandra/googleplaystore.csv");
        
        try {
          lines= Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);  
        }
        catch(IOException e){
                System.out.println("Κάτι πήγε στραβά" + e);
        }
 
        int temp;
        int no_of_replicas = 3, lineCnt = 0;
        String copy = null;
        Chord_node node, temp_chord_node, last_chord_node;    

        // For each chord node in the network
        lineCnt++;
        ArrayList<Long> keys = new ArrayList<>();
        for (int i = 0; i < number_of_chord_nodes; i++) {
            
            String[] array = null;
            for (String line : lines.subList(lineCnt, lines.size() - 1)) { 
                array = line.split(",");
                lineCnt++;
                copy = line;
                break;
            }
            // if we reach eof start from the beggining of the file
            if (lineCnt == lines.size() - 1)
                lineCnt = 1;
            // Get the right key
            long theKey = getHash(array[0]);
            // store it to the map
            keysNvalue.put(theKey,copy);
            //System.out.println("-> " + copy+"   :   "+array[0]+"  "+i);
            keys.add(theKey);
        }
        

        List<Map.Entry<Long, String>> entries = new ArrayList<>(keysNvalue.entrySet());
        
        /*
        Collections.sort(entries, new Comparator<Map.Entry<Integer, String>>() {
          public int compare(Map.Entry< Integer, String> a, Map.Entry<Integer, String> b){
            return a.getKey().compareTo(b.getKey());
          }
        });
        for (Map.Entry<Integer, String> entry : entries) {
          sortedMap.put(entry.getKey(), entry.getValue());
        }
        */
        
        Collections.sort(entries, (Map.Entry< Long, String> a, Map.Entry<Long, String> b) -> a.getKey().compareTo(b.getKey()));
        Map<Long, String> sortedMap = new LinkedHashMap<>();
        entries.stream().forEach((entry) -> {
            sortedMap.put(entry.getKey(), entry.getValue());
        });
        
        // sort the keys
        Collections.sort(keys);
        int i = 0;
        
        // search in keys
        for (Long key: keys) {
            node = node_list.get(i);
            temp_chord_node = node;
            
            
            // till chord node is close to the key but hasn't bigger value
            do {
                
                if(temp_chord_node.successor.node_index < temp_chord_node.node_index /*&& (temp_chord_node.node_index < l)*/) {
                    break;
                }
                
                temp_chord_node = temp_chord_node.successor;
                //System.out.println("loop"+key+" "+temp_chord_node.node_name);
                
            } while ((temp_chord_node.node_index < key));
            
            temp_chord_node = temp_chord_node.successor;
            
            temp_chord_node.key.add(key);
            temp_chord_node.data.add(sortedMap.get(key));
            
            String[] dates = null;
            for (String line : temp_chord_node.data) { 
                dates = line.split(",");
                break;
            }
            
            temp_chord_node.data_date = data_date(dates[11].replaceAll("\"",""));
            
            System.out.println("Node " + temp_chord_node.node_name + "\n" + "has key: " + key);
            System.out.println("and data: " + sortedMap.get(key));
            System.out.println("and date: " + temp_chord_node.data_date);
            
            // iterate for number of replicas
            // temp_chord_node = node;
            for (int j = 0; j < no_of_replicas; j++) {
                temp_chord_node = temp_chord_node.successor;
                temp_chord_node.key.add(key);
                temp_chord_node.data.add(sortedMap.get(key));
                
                dates = null;
                for (String line : temp_chord_node.data) { 
                    dates = line.split(",");
                    break;
                }
                
                temp_chord_node.data_date = data_date(dates[11].replaceAll("\"",""));
            
                System.out.println("Node " + temp_chord_node.node_name + "\n" + "has key: " + key);
                System.out.println("and data: " + sortedMap.get(key));
                System.out.println("and date: " + temp_chord_node.data_date);
            }

            i++;
        }
        
    }
    
    
    // this function is used to find the right node in the chord that is
    // responsible for the key user has typed and show how many steps this
    // procedure needed
    public int searchInChord(long hash_key, boolean searchVersion)
    {        
        int number_of_redirections = 0;
        Chord_node node, curNode, nextNode;
        boolean endSearch = false;
        
        // we pick a random node to initiate our search
        node = randomNode();
        System.out.println("Ξεκινάμε από τον τυχαίο κόμβο: " + node.node_name);
        
        while (true) {           
            // Iterate through the finger table of the chord node
            for (int i = 0; i < node.finger_table.size(); i++) {
                curNode = node.finger_table.get(i);   
                
                // We reached the end of finger table and did not found a suitable candidate yet
                if (i == node.finger_table.size() - 1) {
                    node = curNode;
                    number_of_redirections++;
                    
                    System.out.println("Ανακατεύθυνση στον κόμβο: " + node.node_name);
                    break;
                }
                
                nextNode = node.finger_table.get(i + 1);
                
                // If the hash key falls within the two entries of the finger table
                if ((curNode.node_index < hash_key) && (hash_key <= nextNode.node_index)) {
                    node = curNode;
                    number_of_redirections++;
                    System.out.println("Ανακατεύθυνση στον κόμβο: " + node.node_name);
                    break;
                }  
                
                else if(curNode.key.contains(hash_key) || curNode.successor.key.contains(hash_key)) {
                     node = curNode;           
                     number_of_redirections++;
                     //System.out.println(node.nodeKeys());
                     break;
                }
                else {
                    //node = table_node_1;
                    //number_of_redirections++;
                    //System.out.println("Redirecting to node: a" + node.node_name);
                }

            }
            
            
            // We break out in case we find that current node is responsible for the key
            if (node.key.contains(hash_key)) {
                
                // if user wants latest version of his/her file
                if(searchVersion)
                    node = findLastVersionNode(node, searchVersion, hash_key);
                
                //System.out.println(node.nodeKeys());
                break;
            }
             
            // We break out in case we find that next node is responsible for the key
            if (node.successor.key.contains(hash_key)) {
                // if user wants latest version of his/her file
                if (searchVersion)
                    node = findLastVersionNode(node, searchVersion, hash_key);
                node = node.successor;
                //System.out.println(node.nodeKeys());
                break;
            }
            
            // default chord
            // hash key between the current node index and the successor index
            //if ((node.node_index < hash_key) && (hash_key <= node.successor.node_index)) {
                //node = node.successor;
                //break;
            //}
        }        
        
        // node and key we found and number of redirections
        System.out.println("Ο κόμβος " + node.node_name + " έχει το κλειδί K" + hash_key);
        System.out.println("Αριθμός ανακατευθύνσεων: " + number_of_redirections);
        
        return number_of_redirections;
    }
    
    
    // iterate the chord to find if another node with more recent version exist
    // not the best way to solve the problem but it works
    private Chord_node findLastVersionNode(Chord_node node, boolean searchVersion, long hash_key)
    {
        ArrayList<Chord_node> nodesNeedUpdate = new ArrayList<>();
        
            //for num of replicas
            for (int rep = 0; rep < number_of_chord_nodes; rep++)
                // if another node has same key with more recent version
                if(node.successor.key.contains(hash_key) && node.successor.data_date > node.data_date) {
                    node = node.successor;
                    nodesNeedUpdate.add(node);
                }
            
            // update the version for all the nodes that had older versions of the same app
            for (Chord_node cn: nodesNeedUpdate) {
                cn.data = node.data;
                cn.data_date = node.data_date;
            }
            
        return node;
    }
    
    
    private Chord_node randomNode()
    {
        int min = 0;
        int max = number_of_chord_nodes - 1;
        
        // generate a random number between 0 and the number of chord nodes
        int randomNum = rand.nextInt((max - min) + 1) + min;
        
        return get(randomNum);
    }
    
    
    // return the chord node at the given index
    private Chord_node get(int index)
    {
        return node_list.get(index);
    } 
    

    
    protected class Chord_node 
    {
        // m inputted by the user
        public final int m = Cassandra.getNumberOfIPS();

        Chord_node successor;                   // node's successor
        Chord_node predecessor;                 // node's predecessor
        ArrayList<Chord_node> finger_table;     // finger table
        ArrayList<Long> key;                 // keys that node is responsible for
        ArrayList<String> data;                 // data that each key has
        long node_index;                         // index of this node [0 to (2^m-1)]
        public String node_name;                // Name of node, e.g., N35   
        public double data_date;                // date of key to define version

        // Initialize everything
        public Chord_node(long node_index)
        {            
            this.node_index = node_index;
            this.node_name = "N" + Long.toString(node_index);
            finger_table = new ArrayList<>();
            key = new ArrayList<>();
            data = new ArrayList<>();
        }
        
        // get the name of the node
        @Override
        public String toString()
        {
            return node_name;
        }
        
        // get keys that node is responsible for
        public String nodeKeys()
        {
            StringBuilder builder = new StringBuilder();
            for(Long s : key) 
                builder.append(s == key.get(key.size()-1)? s + "" : s + ",");
            String str = builder.toString();
            return "O " + node_name + " είναι υπεύθυνος για τα κλειδιά: " + str;
        }
    }
    
    
    // find the version using the date of our data
    public static double data_date(String dataString)
    {
        // split the string to month and day of the month
        // every app was last updated in 2018 so we didn't take into
        // consideration the year but it wouldn't be much work to add it too
        String[] array = dataString.split(" ");
        System.out.println(array[0]);
        Date date = null;
        
            try {
                date = new SimpleDateFormat("MMMMM", Locale.ENGLISH).parse(array[0]);
            } catch (ParseException ex) {
                // if date format has trash inside it just return 1 to the version
                return 1;
            }
            
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);
        double dayOfMonth = (double) 0.1*Double.parseDouble(array[1]);
        dayOfMonth = (double) Math.round(dayOfMonth * 100d) / 100d;
        
        // System.out.println(month == Calendar.JANUARY); 
        
        // return the version as a real number 
        return (double) ( month + dayOfMonth );
  }
   
    
    // use a comparator to sort the nodes according to their indexes
    protected class Chord_node_comparator implements Comparator<Chord_node> 
    {
        @Override
        public int compare(Chord_node node1, Chord_node node2)
        {
            
            return ( (node1.node_index  > node2.node_index)? 1 : -1 );
            
        }
    }
    

}