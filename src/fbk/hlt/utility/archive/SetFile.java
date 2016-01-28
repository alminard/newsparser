package fbk.hlt.utility.archive;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.*;
import java.io.*;

/**
 * User: cgirardi
 * Date: 1-feb-2007
 * Time: 21.23.41

 Strategia:
 - tengo aperto il canale del file, mantengo all'inizio del file tre numeri per sapere:
 * il numero di chiavi ordinate
 * il numero di chiavi non ancora ordinate (che si trovano in fondo al file)
 * la posizione dopo l'ultima chiave ordinata (da dove vengono salvate le chiavi non ordinate)

 - mantengo un hash con le chiavi non ordinate per trovarle + velocemente. quando il file viene chiuso
 effettuo il merge ordinando tutte le chiavi.

 * 2010.02.24
 * - migliorato il get e il contains (utilizzo di cache e un binarySearch)
 * - per la normalizzazione delle chiavi uso new String(key.getBytes()) così vengono ordinate correttamente anche stringhe UTF8

 */
public class SetFile implements Iterable<String> {
    private Hashtable<String, String> newkeys = new Hashtable<String,String>();
    private LinkedHashMap<String,Long> goodjumpposition = new LinkedHashMap<String,Long>();
    private File file;
    private RandomAccessFile rafile;
    private boolean isReadable = false;
    private String[] cache = {"",""};
    //private int countmergedfiles;
    private static final String SEPARATOR = "@";
    private long availableMemory = 0;
    private long unsortedPos = 0;
    private int unsortedSize = 0;
    //private static final String DELSEPARATOR = "§";

    /*private long heapMaxSize;
    private static int countHeapUse;
    private static int countHeapInit = 20;
    */

    boolean DEBUG = false;
    static final String RETURN_TAG = "<_n>";


    public SetFile(String filepath) throws Exception {
        file = new File(filepath);
        if (DEBUG)
            System.err.println("SETFILE: " +file);
        //if  (filepath.endsWith("webdatelogging.set"))
        //  DEBUG=true;

        if (!file.exists()) {
            this.clear();
        } else {
            try {
                if (file.canRead()) {
                    if (file.canWrite()) {
                        rafile = new RandomAccessFile(file.getCanonicalPath(),"rw");
                    } else {
                        rafile = new RandomAccessFile(file.getCanonicalPath(),"r");

                    }
                    isReadable = true;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        this.unsortedPos = this.readUnsortedPosition();
        this.unsortedSize = this.readUnsortedSize();
        if (unsortedSize == 0) {
            try {
                if (this.unsortedPos != rafile.length())
                    System.err.println("# WARNING! The file " + file + " is corrupted (maximum length exceed).");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                rafile.seek(unsortedPos);
                String line;
                //while((line = rafile.readLine()) != null) {
                while((line = rafile.readUTF()) != null) {
                    if (line.lastIndexOf(SEPARATOR) > 0)
                        newkeys.put(line.substring(0,line.lastIndexOf(SEPARATOR)),line.substring(line.lastIndexOf(SEPARATOR) +1));
                    rafile.readLine();
                    //System.out.println("# "+line + " " + rafile.getFilePointer() + " " + rafile.length());
                    if (rafile.getFilePointer() >= rafile.length()) {
                        break;
                    }
                }
                //controllo che le entry dell hash siano uguali a unsortedSize
                if (unsortedSize != newkeys.size()) {
                    System.err.println("# WARNING! The file " + file + " is corrupted (unsorted keys should be " + unsortedSize + ").");
                }
            } catch (IOException e) {
                System.err.println("# WARNING! The file " + file + " is corrupted (unsorted keys failed: " + unsortedSize + ").");
            }
            this.merge();
        }
        availableMemory = Runtime.getRuntime().freeMemory();
        setGoodJumps();
        //System.err.println("SetFile " + file + " (" +this.size() + "," + this.readUnsortedSize()+ "," +this.readUnsortedPosition()+")");
        /*heapMaxSize = Runtime.getRuntime().maxMemory();
        SetFile.countHeapUse = SetFile.countHeapInit - Long.numberOfTrailingZeros(heapMaxSize / Runtime.getRuntime().freeMemory());
        heapMaxSize = heapMaxSize / SetFile.countHeapInit;
        */
    }

    private void setGoodJumps () throws IOException {
        goodjumpposition.clear();
        if (this.size() > 10000) {
            readUnsortedPosition();
            long currpos = rafile.getFilePointer();
            //String line = rafile.readLine();
            String line = rafile.readUTF();
            goodjumpposition.put(line.substring(0,line.lastIndexOf(SEPARATOR)), currpos);
            int rate = (int) rafile.length() / 2000;
            for (int i=1; i < 2000; i++) {
                rafile.seek(rate * i);
                rafile.readLine();
                //rafile.readUTF();
                long pos = rafile.getFilePointer();
                //line = rafile.readLine();
                line = rafile.readUTF();
                goodjumpposition.put(line.substring(0,line.lastIndexOf(SEPARATOR)), pos);

            }
        }
        //System.err.println("goodjumpposition : " + goodjumpposition.size());
    }

    private long getMinPosition (String key) throws IOException {
        long min = 0;
        for (Map.Entry entry : goodjumpposition.entrySet()) {
            if (((String) entry.getKey()).compareTo(key) < 0) {
                min = (Long) entry.getValue();
            } else {
                if (min != 0) {
                    //System.err.println("getMinPosition="+min + " " +key);
                    return min;
                } else {
                    break;
                }
            }
        }
        readUnsortedPosition();
        return rafile.getFilePointer();
    }

    public boolean contains (String key) {
        if (get(key) != null) {
            return true;
        }
        return false;
    }

    public synchronized String get(String key) {
        if (key != null) {
            key = key.replaceAll("[\n|\r]+","").trim(); //.replaceAll("\\p{Cntrl}","")
            if  (key.length() > 0) {
                key = new String(key.getBytes());
                //return this.binarySearch(key);
                if (cache[0].equals(key)) {
                    return cache[1];
                } else {
                    cache[0] = key;

                    //effettuo una ricerca binaria nei record salvati su file
                    cache[1] = this.binarySearch(key);
                    return cache[1];
                }
            }
        }
        return null;
    }



    public synchronized boolean put(String key, String value) throws Exception {
        if (key != null) {
            key = key.replaceAll("[\n|\r]+","").trim();
            if  (key.length() > 0) {
                key = new String(key.getBytes());

                //if (!this.contains(key)) {
                value = new String(value.getBytes());
                value= value.replaceAll("[\n|\r]+", RETURN_TAG).trim();
                if (DEBUG)
                    System.err.println("PUT " + key +", " + value + " ("+newkeys.size()+")");
                try {
                    rafile.seek(rafile.length());
                    //rafile.writeBytes(key + SEPARATOR + value +"\n");
                    rafile.writeUTF(key + SEPARATOR + value);
                    rafile.writeBytes("\n");
                    writeUnsortedSize(++unsortedSize);
                } catch (IOException e) {
                    return false;
                }
                cache[0] = key;
                cache[1] = value;
                try {
                    newkeys.put(key,value);
                    if (newkeys.size() % 10000 == 0 && Runtime.getRuntime().freeMemory() < availableMemory / 2) {
                        merge();
                    }

                } catch(Exception e) {
                    System.err.println("newkeys.size() = "+newkeys.size() );
                }

                return true;
                //}
            }

        }
        return false;
    }

    //merge hashfile and file already saved
    public synchronized void merge () throws Exception {
        if (newkeys.size() > 0) {
            //System.err.println("MERGE newkeys.size() = "+newkeys.size() +" Tot MEMORY=" + Runtime.getRuntime().totalMemory() +" FREE MEMORY=" + Runtime.getRuntime().freeMemory());
            if (DEBUG)
                //System.err.println(countmergedfiles+ " Adding " + newkeys.size() + " keys");
                System.err.println("# Adding " + newkeys.size() + " new keys");
            List<String> listaddedkeys = Collections.list(newkeys.keys());
            Collections.sort(listaddedkeys);
            try {
                //countmergedfiles++;
                //File output = new File(file.getCanonicalPath() + countmergedfiles + ".mrg");
                File output = new File(file.getCanonicalPath() + ".mrg");
                RandomAccessFile outmrg = new RandomAccessFile(output,"rw");
                int rasize =  this.size();
                outmrg.seek(0);
                outmrg.writeInt(rasize);
                outmrg.writeInt(0);
                outmrg.writeLong(0);

                rafile.seek(0);
                rafile.readInt();
                rafile.readInt();
                rafile.readLong();

                //String line = rafile.readLine();
                String line = rafile.readUTF();
                rafile.readLine();
                int hashpos = 0;
                int compareCondition;
                String newline = "";
                int savedCounter = 0;
                while (isReadable) {
                    if (line == null || line.length() == 0 || this.unsortedPos < rafile.getFilePointer()) {
                        if (newline.length() > 0) {
                            savedCounter++;
                            //outra.writeBytes(newline + "\n");
                            outmrg.writeUTF(newline);
                            outmrg.writeBytes("\n");
                        }

                        break;
                    } else {
                        if (hashpos < listaddedkeys.size()) {
                            int pos = line.lastIndexOf(SEPARATOR);
                            if (pos > 0 && listaddedkeys.get(hashpos) != null) {
                                compareCondition = line.substring(0,pos).compareTo(listaddedkeys.get(hashpos));
                                if (compareCondition < 0) {
                                    newline=line;
                                    //line = rafile.readLine();
                                    System.err.println("rafile: "+ rafile.getFilePointer() + " " +rafile.length());
                                    if (rafile.getFilePointer() < rafile.length())  {
                                        line = rafile.readUTF();
                                        rafile.readLine();
                                    } else
                                        line = null;
                                } else if (compareCondition > 0) {
                                    newline = listaddedkeys.get(hashpos) + SEPARATOR + newkeys.get(listaddedkeys.get(hashpos));
                                    hashpos++;
                                } else {
                                    newline = line.substring(0,pos) + SEPARATOR + newkeys.get(listaddedkeys.get(hashpos));
                                    hashpos++;
                                    //line = rafile.readLine();
                                    if (rafile.getFilePointer() < rafile.length()) {
                                        line = rafile.readUTF();
                                        rafile.readLine();
                                    } else
                                        line = null;                                }
                            } else {
                                throw new Exception("# WARNING! The file " + file + " is corrupted (line separator is missed)\n" + rafile.getFilePointer() + ": " + line);
                            }
                        } else {
                            newline = line;
                            //line = rafile.readLine();
                            if (rafile.getFilePointer() < rafile.length()) {
                                line = rafile.readUTF();
                                rafile.readLine();
                            } else
                                line = null;                        }
                    }
                    savedCounter++;
                    //outra.writeBytes(newline + "\n");
                    outmrg.writeUTF(newline);
                    outmrg.writeBytes("\n");

                    newline="";
                    //numkey++;
                    //System.err.println(a + " && " + hashpos + " >= " +  listaddedkeys.size());

                }
                for (int i=hashpos; i<listaddedkeys.size(); i++) {
                    savedCounter++;
                    //outra.writeBytes(listaddedkeys.get(i) + SEPARATOR + newkeys.get(listaddedkeys.get(i)) +"\n");
                    outmrg.writeUTF(listaddedkeys.get(i) + SEPARATOR + newkeys.get(listaddedkeys.get(i)));
                    outmrg.writeBytes("\n");
                }

                this.unsortedPos = outmrg.getFilePointer();

                if (savedCounter != rasize) {
                    System.err.println("# WARNING! The size should be " + rasize + " ("+file.getCanonicalPath()+ " lines are " + savedCounter + ")");
                }
                outmrg.seek(0);
                outmrg.writeInt(savedCounter);
                outmrg.writeInt(0);
                outmrg.writeLong(this.unsortedPos);
                outmrg.close();

                rafile.close();
                rafile = null;

                if (file.delete()) {
                    if (!output.renameTo(file)) {
                        System.err.println("WARNING! Rename failed " + output);
                    }

                } else {
                    System.err.println("WARNING! The file "+file+" cannot be deleted.");
                }
                reset();

                //System.err.println(file.delete() +"  merge kyb: " + );
                rafile = new RandomAccessFile(file,"rw");
                isReadable = true;

                setGoodJumps();
                newkeys.clear();
                this.unsortedSize = 0;
                this.writeUnsortedSize(this.unsortedSize);

            } catch (FileNotFoundException e) {
                System.err.println("ERROR! Set file hasn't found! " +e.getMessage());
                //e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IOException e) {
                System.err.println("ERROR! Failed merging on file " + file + ": " +e.getMessage());
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                System.exit(0);
            }
        }
    }

    private void reset() {
        Runtime.getRuntime().gc();
        //System.err.println("RESET newkeys.size() = "+newkeys.size() +" Tot MEMORY=" + Runtime.getRuntime().totalMemory() +" FREE MEMORY=" + Runtime.getRuntime().freeMemory());
    }


    // in this version there is an optimization per fare backward solo nel caso si passi per 2 volte dalla stessa linea
    // c'e` ancora qualcosa che non va (vedi test YouTubeDown cR4zRbPy2kY)

    // scaricare HGxbidvsg_g, x4DmiNalLvA, fdv21GPsyBQ
    private synchronized String binarySearch(String str) {
        if (!isReadable)
            return null;

        if (newkeys.containsKey(str)) {
            if (DEBUG)
                System.err.println("hash searching " +str);

            return newkeys.get(str);
        } else {
            try {

                String line, substr;

                //parto da dove sono per risparmiare tempo
                //System.err.println (rafile.getFilePointer() + "> " + str);

                //se faccio la ricerca partendo dall'ultima posizione letta
                // (todo sembra che non si risparmi nulla anzi le prestazioni peggiorano leggermente, da RIVEDERE)
                /*line = rafile.readLine();
               if (line != null ) {
                   substr = line.substring(0,line.lastIndexOf(SEPARATOR));
                   if (str.equals(substr)) {
                       return line.substring(line.lastIndexOf(SEPARATOR) + SEPARATOR.length());
                   } else {
                       //System.err.println(line.substring(0,line.indexOf(SEPARATOR)) + ")).compareTo(" + str + ")");
                       if (substr.compareTo(str) < 0) {
                           //System.err.println(line + "> " + str);
                           min = rafile.getFilePointer();
                           max = this.unsortedPos;

                       } else {
                           //System.err.println(line + "< " + str + " " +min +"<=" + max + " && " + mid + " < " + (max - 1));
                           max = rafile.getFilePointer();

                       }
                       rafile.seek(min);
                   }
               } else {
                   max = this.unsortedPos();
               } */

                if (DEBUG)
                    System.err.println("SEARCHING " +str);
                readUnsortedPosition();
                long startPointer = rafile.getFilePointer();
                long min = startPointer;
                long max = this.unsortedPos;
                long mid= (min+max) / 2;

                //rafile.seek(min);
                byte ch;
                while (mid > startPointer && mid < (max - 1)) {
                    //System.err.println("\n1min: " +min + ", mid: " + mid + ", max: " +max + ", filePointer="+rafile.getFilePointer());
                    mid = rafile.getFilePointer();
                    while (rafile.getFilePointer() > startPointer) {
                        rafile.seek(rafile.getFilePointer() -2);
                        ch = (byte) rafile.read();

                        if (ch == '\n' || ch == '\r') {
                            break;
                        }
                    }

                    if (rafile.getFilePointer() < startPointer) {
                        rafile.seek(startPointer);
                    }

                    //if ((line = rafile.readLine()) != null) {
                    if ((line = rafile.readUTF()) != null) {
                        if (DEBUG)
                            System.err.println("FIND " + startPointer + "," + rafile.getFilePointer() + " (" + line +")");
                        if (line.lastIndexOf(SEPARATOR) > 0) {
                            substr = line.substring(0,line.lastIndexOf(SEPARATOR));

                            if (str.equals(substr)) {
                                if (DEBUG)
                                    System.err.println("FOUND {{" +str + "}}");
                                return line.substring(line.lastIndexOf(SEPARATOR) + SEPARATOR.length()).replaceAll("[\n|\r].*","");
                            } else {
                                if (substr.compareTo(str) < 0) {
                                    //System.err.println("SET MIN: " + line + "> " + str);
                                    //min = rafile.getFilePointer()+1;
                                    //min = rafile.getFilePointer();
                                    min = mid;
                                } else {
                                    //System.err.println("SET MAX: " + line + "> " + str);
                                    //System.err.println(line + "< " + str + " " +min +"<=" + max + " && " + mid + " < " + (max - 1));
                                    max = mid;
                                    //max = mid;
                                }
                            }
                        }

                        //if (max == rafile.getFilePointer()) {
                        //  break;
                        //}
                        mid = (min+max) / 2;
                    } else {
                        break;
                    }
                    rafile.seek(mid);
                }

            } catch (IOException e ) {
                //e.printStackTrace();
                System.err.println("Warning! Binary search error on file: " +file);
            }
        }
        return null;
    }

    public void close() {
        try {
            //merge();
            isReadable = false;

            rafile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized int size() {
        try {
            rafile.seek(0);
            return (rafile.readInt() + rafile.readInt());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private synchronized int readSize() {
        try {
            rafile.seek(0);
            return rafile.readInt();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private synchronized void writeSize(int num) {
        try {
            rafile.seek(0);
            rafile.writeInt(num);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized int readUnsortedSize() {
        try {
            readSize();
            return rafile.readInt();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private synchronized void writeUnsortedSize(int num) {
        try {
            readSize();
            rafile.writeInt(num);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized long readUnsortedPosition() {
        try {
            size();
            return rafile.readLong();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 16;
    }

    private synchronized void writeUnsortedPosition(long num) {
        try {
            size();
            rafile.writeLong(num);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String toString() {
        return file + " (" +this.readSize() + "," + this.readUnsortedSize() + ","+this.readUnsortedPosition()+")";
    }

    public synchronized void clear() {
        try {
            if (file != null) {
                if (rafile != null)
                    rafile.close();
                if (file.exists()) {
                    file.delete();
                    //System.err.println("Creating setfile: " + file.getCanonicalPath());
                }
            }

            file.createNewFile();
            rafile = new RandomAccessFile(file.getCanonicalPath(),"rw");
            this.writeSize(0);
            this.writeUnsortedSize(0);
            this.writeUnsortedPosition(16);

            isReadable = true;

            //rafile.setLength(0);
            //System.err.println("Controllo lo stato di rafile (" + file.getName() + "): ");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //Iterable interface
    public synchronized Iterator<String> iterator() {
        try {
            merge();
            size();
            rafile.readLong();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            return new Iterator<String>() {
                long currpos = rafile.getFilePointer();
                public boolean hasNext() {
                    try {
                        if (rafile.getFilePointer() != currpos)
                            rafile.seek(currpos);
                        //System.err.println("SetFile hasNext(): " +rafile.getFilePointer() +"<"+ rafile.length());
                        return (currpos < rafile.length());
                    } catch (IOException e) {
                        return false;
                    }
                }
                public String next() {
                    try {
                        if (rafile.getFilePointer() != currpos)
                            rafile.seek(currpos);
                        //String line=rafile.readLine();
                        String line=rafile.readUTF();
                        rafile.readLine();

                        currpos = rafile.getFilePointer();
                        //System.err.println("SetFile next(): " +rafile.getFilePointer() +" "+ line);
                        if (line.indexOf(SEPARATOR) > 0)
                            return line.substring(0, line.lastIndexOf(SEPARATOR));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //test the class
    public static void main(String[] args) {
        // check parameters
        Calendar c = Calendar.getInstance();
        long todayInMillis = c.getTimeInMillis();
        int num = 0;
        int num_null = 0;
        try {
            if (args.length < 1) {
                System.err.println("No input file found.");
                return;
            } else if (args[0].equalsIgnoreCase("-test")) {
                String strfile ="/tmp/_test.set";
                File tmpfile = new File(strfile);
                if (tmpfile.exists())
                    tmpfile.delete();
                SetFile setfile = new SetFile(strfile);
                setfile.put("http://en.wikinews.org/wiki/Apple_introduces_new_iPod_with_video_playback_capabilities", "13");
                setfile.put("http://en.wikinews.org/wiki/Apple_executive_leaves_company_after_iPhone_4_antenna_issues", "11691");
                setfile.put("http://en.wikinews.org/wiki/Apple_Inc._CEO_Steve_Jobs_on_medical_leave","13555");
                setfile.put("http://en.wikinews.org/wiki/Apple_executive_Steve_Jobs_resigns", "15348");
                setfile.put("http://en.wikinews.org/wiki/Apple_Inc._co-founder_Steve_Jobs_dies_aged_56", "18352");
                setfile.close();

                setfile = new SetFile(strfile);

                if (!setfile.contains("http://en.wikinews.org/wiki/Apple_Inc._CEO_Steve_Jobs_on_medical_leave")) {
                    setfile.put("http://en.wikinews.org/wiki/Apple_Inc._CEO_Steve_Jobs_on_medical_leave","21601");
                    System.err.println("An item doesn't found!");
                }
                //setfile.put("http://en.wikinews.org/wiki/Apple_Inc._CEO_Steve_Jobs_on_medical_leave","1");

                if (setfile.size() == 5 && setfile.contains("http://en.wikinews.org/wiki/Apple_Inc._CEO_Steve_Jobs_on_medical_leave")) {
                    System.err.println("TEST PASSED!");
                } else {
                    System.err.println("TEST FAILED!");
                }
                setfile.close();

            } else if (args.length == 2 && args[0].equalsIgnoreCase("-l")) {
                SetFile setfile = new SetFile(args[1]);
                Iterator it = setfile.iterator();
                String key, val;

                while (it.hasNext()) {
                    key = (String) it.next();
                    val = setfile.get(key);
                    if(val == null)
                        num_null++;
                    System.err.println("> " +key + " " + val);
                    num++;
                }
                setfile.close();
            } else {

                HashMap<String,Integer> words = new HashMap<String,Integer>();
                SetFile setfile;
                String line,setvalue;
                //boolean alreadyMade = false;

                //se ho un file come secondo parametro carico le sue linne come entry da testare ,altrimenti uso il .set stesso
                String filein = args[0];
                boolean isSetFile = true;

                if (args.length > 1) {
                    filein = args[1];
                    isSetFile = false;
                }
                InputStreamReader insr = new InputStreamReader(new FileInputStream(new File(filein)),"UTF8");

                BufferedReader in = new BufferedReader(insr);
                in.readLine();
                while ((line = in.readLine()) != null) {
                    String key = line;
                    if (isSetFile)
                        key= line.substring(0,line.lastIndexOf(SEPARATOR));
                    if (key.length() > 0) {
                        // System.err.println(items[i]);
                        if (words.containsKey(key)) {
                            System.err.println("WARNING! Double value found... "+key);
                            //words.put(items[i],(Integer) words.get(items[i]) +1);
                        } else {
                            //System.err.println(items[i]);
                            if (isSetFile)
                                words.put(key, Integer.valueOf(line.substring(line.lastIndexOf(SEPARATOR)+SEPARATOR.length()).replaceAll("[\n|\r].*","")));
                            else
                                words.put(key, -1);
                        }

                    }

                }
                in.close();
                insr.close();
                setfile = new SetFile(args[0]);
                // setfile.close();

                Set keyset = words.keySet();
                Iterator keys = keyset.iterator();
                int error =0;
                Date stime = new Date();
                int counter = 0;
                while(keys.hasNext()) {
                    line = (String) keys.next();
                    counter++;
                    if (counter % 100 == 0)
                        System.err.println(counter+"/"+keyset.size() + " " +line);

                    setvalue = setfile.get(line);
                    if (setvalue != null) {
                        if (words.get(line) != -1 && words.get(line) != Integer.parseInt(setvalue)) {
                            System.err.println(line + " " + setfile.get(line) + " != " +(Integer) words.get(line));
                            error++;
                        }
                    } else {
                        System.err.println("ERROR: Entry " + line + " not found.");
                        error++;
                    }
                }
                setfile.close();
                Date now = new Date();
                System.err.println("Time: " + (now.getTime() - stime.getTime()) + "ms");
                System.err.println(keyset.size() + " entries found");
                System.err.println("Error: " +error);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        c = Calendar.getInstance();
        System.err.println("Time: " + (c.getTimeInMillis() - todayInMillis) + " ("+num_null+"/"+num+" errors)");
    }

}
