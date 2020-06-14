package cassandra;

import java.io.File;
import java.io.IOException;
import static java.lang.System.exit;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class Cassandra
{
    private static int nodesNumber, ipsNumber;
    private static List<String> lines;
    private static String idd;
    private static ArrayList<Integer> redirectionsNumbers = new ArrayList<>();
    
    public static void main(String[] args) 
    {
        boolean ever = true;
        Randips ips = new Randips();
        String idString = null;
        long hashKey;
        int RoundNO = 0, totalRedirections = 0;    
         
        File file = new File("src/cassandra/googleplaystore.csv");
        try {
          lines= Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);  
        }
        catch(IOException e){
                System.err.println("Δεν βρέθηκε το αρχείο!" + e);
        }

        System.out.println("Δώσε έναν αριθμό για κόμβους!");
        Scanner f = new Scanner(System.in);
        nodesNumber = f.nextInt();
        
        // clean scanner
        f.nextLine();

        System.out.println("Δώσε έναν αριθμό για τις διευθυνσεις!");
        ipsNumber = f.nextInt();

    
        System.out.println("Οι ip διευθύνσεις και τα ports τους ειναι:");
        
        Chord chord_network = new Chord();
        
        for(;ever;)
        {
            Scanner sc = new Scanner(System.in);
            boolean found = false;
            int j = 0;

            do {
                System.out.println("\n> Πληκτρολόγησε τη λέξη \"stop\" ή \"STOP\" για να τερματίσεις το πρόγραμμα");
                System.out.print("> Πληκτρολόγησε το όνομα της εφαρμογής: ");

                try{
                    idString = sc.nextLine();
                    
                } catch(Exception e) {
                    System.out.println(e);
                }
                
                if (idString.equalsIgnoreCase("help")) { 
                    showHelp(); 
                    continue; 
                }
                
                if (idString.equalsIgnoreCase("stop")) { 
                    averageRedirections(totalRedirections, RoundNO); 
                    exit(0); 
                }
                
                idd = idString;

                for (String line : lines) {
                    j++;
                    if( j == 1 ) continue;
                    String[] array = line.split(",");
                    
                    if(idString.equals(array[0])) {
                        found = true;
                        System.out.println("Η γραμμή που βρέθηκε το όνομα του αρχείου είναι: " + (j+1) + "\n");
                        break;
                    } 
                }
                
                if (!found)  System.out.println("Το αρχείο δεν υπάρχει σε κανένα κόμβο του δαχτυλιδιού");
                
            } while(!found);

            
            RoundNO++;
            
            //Get the hash key of the app id 
            hashKey = chord_network.getHash(idString);         
            System.out.println("Τιμή κατακερματισμού του id της εφαρμογής: " + "K" + hashKey);
            
            // Search until the hash key maches the right key in the chord
            // second parameter is true if we'd like latest version of the file
            // or false if we dont care about the version  
           int redirections = chord_network.searchInChord(hashKey, true);            
           
           // total redirections to compute later average redirections
           totalRedirections += redirections;
           
           // we add the numbers to an arraylist to compute the median at the end
           redirectionsNumbers.add(redirections);
        }
        
        averageRedirections(totalRedirections, RoundNO); 
        
    }   
    
    
    public static void showHelp() 
    {
        System.out.println("\n\n")  ;
        System.out.println("  .88bd88.                    CHORD PROTOCOL                             ");
        System.out.println(" d88'  `88b                                                              ");
        System.out.println("888      888                  One  ring  to  rule them all,               ");
        System.out.println("888      888                  One  ring  to  find them all,              ");
        System.out.println("888      888                  One  ring  tο bring them all,              ");
        System.out.println("`88b    d88'                  And in the darkness bind them              ");
        System.out.println(" `88bood88'                                Gandalf,The Grey              ");
        System.out.println("\n\n");
        System.out.println("Οδηγίες\n                                                                ");
        System.out.println("- Πληκτρολόγησε το όνομα του αρχείου  που θες να ψάξεις στο  δαχτυλίδι   ");
        System.out.println("- Αν κάποιος κόμβος έχει το αρχείο θα δεις βήμα βήμα τη διαδικασία της   ");
        System.out.println("- εύρεσης - προσομοίωσης  να πραγματοποιείται και τελικά θα εμφανιστεί   ");
        System.out.println("- μήνυμα με τον κόμβο που είναι υπεύθυνος για το κλειδί σύμφωνα με  το   ");
        System.out.println("- πρωτόκολλο και ταυτόχρονα έχει την τελευταία έκδοση των δεδομένων\n\n  ");
        System.out.println("--  Δημιουργοί  --  \n                                                   ");
        System.out.println("Γαλάτης Αθανάσιος      Α.Μ: 2022201500017                                ");
        System.out.println("Βακουφτσής Αθανάσιος   Α.Μ: 2022201500007                                ");
        System.out.println("\n");
    }

    public static void averageRedirections(int totalRedirections, int RoundNO) 
    {
        System.out.println("");
        // if user stops the programm before making any searches
        if (RoundNO == 0) {
            System.out.println("There weren't any searches");
            return;
        }
        
        // calculate mean
        double exactMean = totalRedirections / (double) RoundNO;
        exactMean = (double)Math.round(exactMean * 1000d) / 1000d;
        
        // calculate the median 
        Collections.sort(redirectionsNumbers);
        double median;
        if (redirectionsNumbers.size() % 2 == 0)
            median = (double)(redirectionsNumbers.get(redirectionsNumbers.size() / 2) + redirectionsNumbers.get(redirectionsNumbers.size() / 2 - 1)) / 2.0;
        else
            median = (double) redirectionsNumbers.get(redirectionsNumbers.size() / 2);
        
        // print the results
        System.out.println("-> Average number of redirections: " + (totalRedirections / RoundNO));
        System.out.println("-> A more exact mean of redirections: " + exactMean);
        System.out.println("-> Median of redirections: " + median);
        System.out.println("ciao");
    }
    
    // make the right getters
    public static String getIdd() {
        return idd;
    }

    public static int getNumberOfNodes() {
        return nodesNumber;
    }
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
    public static int getNumberOfIPS() {
        return ipsNumber;
    }
  
    
}
