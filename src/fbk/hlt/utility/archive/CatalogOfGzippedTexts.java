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

import java.io.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import java.text.ParseException;


/**
 * User: Christian Girardi (cgirardi@fbk.eu)
 * Date: 4-apr-2008
 *
 * STRATEGY
 * Colleziono una serie di file in modo sequenziale mantenendo per ognuno
 * - le info della posizione del prossimo file
 * - le info (filename, size, encoding, ...) del documento corrente
 * il tutto è codificato in una linea speciale i cui valori sono divisi dal separator (i primi due
 * valori sono sempre la posizione del successivo file, cioè la fine del file corrente, e il nome file o url address);
 * Es.
 * - nextpos#:URL#:dtime=Fri Oct 23 12:16:03 CEST 2009#:encoding=#:content-type=text/html; charset=utf-8#:17673...

 * 2010.02.24
 * - attraverso la var initialLen riesco a sapere se un archivio è già in uso
 * - cambiare E MMM dd HH:mm:ss Z yyyy in time senza problemi di codifica Locale

 * ver. 1.2.2
 * - il primo long nel file (posizione 0) indica la posizione dell'ultimo carattere scritto (questo serve ad avere un controllo tra
 * la dimensione del file e l'ultimo carattere dell'ultimo articolo scritto correttamente).
 * Inoltre ogni key è preceduta da un newline (per rendere più facile il checking in caso di errori e disallinemaenti
 * */

public class CatalogOfGzippedTexts  {
    //parser time like: Sat Oct 11 01:30:53 CEST 2008
    SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
    private boolean isWritable = false;

    private long nextPosition = 0;
    private long startPosition = 0;
    //private Hashtable texthash = new Hashtable();
    private SetFile setfile;
    private RandomAccessFile cgtfile ;

    private int itemCounter = 0;
    static private final String separator = "#:";

    private File cgt = null;
    private static int errorno = 0;
    private static int MAXSTEP = 4;

    //mantengo il lenght nel momento dell'apertura. Quando sto per scrivere controllo che la lunghezza non sia
    // cambiata così capisco se qualche altro processo ha fatto delle modifiche
    private long initialLen = -1;


    /** Creates an archive stream to read from, and optionally to write to, a file with the specified name.
     * @param filepath, mode
     */
    public CatalogOfGzippedTexts(String filepath, String mode) throws Exception {
        cgt = new File(filepath);
        String cgtpath = cgt.getCanonicalPath();
        String line;
        try {
            if (!cgt.exists()) {
                if (cgt.getParentFile() != null && !cgt.getParentFile().exists() && !cgt.getParentFile().mkdirs()) {
                    System.err.println("# ERROR! Unable to create directory " + cgt.getParent());
                    throw new EmptyStackException();
                }
            }
            cgtfile = new RandomAccessFile(filepath, mode);
            if (cgtfile.length() == 0) {
                //System.err.println("PASSO 1: creating a new archive  (" +filepath +")" + cgt.getParentFile());


                cgt.createNewFile();
                //posizione dell'header dell'ultimo documento valido
                cgtfile.writeLong(0);
                //numero totale di documento salvati nell'archivio
                cgtfile.writeInt(0);
                cgtfile.writeBytes("\n");

                startPosition = cgtfile.getFilePointer();
                cgtfile.seek(0);
                cgtfile.writeLong(startPosition);

            } else {
                //System.err.println("PASSO 3: update old version archive (" +filepath +")");
                initialLen = cgtfile.length();

                cgtfile.readLong();
                cgtfile.readInt();
                cgtfile.readLine();
                startPosition = cgtfile.getFilePointer();

                if (cgtfile.length() > startPosition) {
                    //se dal controllo tra la prima posizione disponibile (lastpos) e la dimensione del file risulta che questi due numeri non sono uguali
                    // allora setto la lunghezza del file a lastpos e cancello il file .set (che verrà ricreato a partire dal file .cgt)
                    int found=0;
                    int found_ver142=0;
                    int step=1;
                    try {
                        while ((line = cgtfile.readUTF()) != null ) {
                            //System.err.println("LINE: " +line);

                            if (line.matches(".*\\d+" + separator + ".+"+separator+"\\d+")) {
                                cgtfile.seek(cgtfile.getFilePointer() +
                                        Long.parseLong(line.substring(0,line.indexOf(separator))));

                                found++;
                                if (!line.matches("^\\d+" + separator + "[\\+|-].+"+separator+"\\d+")) {
                                    found_ver142++;
                                }
                            }
                            if (cgtfile.getFilePointer()>=cgtfile.length() || step > MAXSTEP)
                                break;
                            step++;
                        }
                    } catch (UTFDataFormatException e) {
                        found_ver142++;
                    }

                    if (found < step) {
                        if (found_ver142 > 0) {
                            System.err.println("# WARNING! This archive seems the old version 1.4.2.");
                        } else {
                            System.err.println("# WARNING! Archive seems corrupted and some entries are not reachable.");
                        }
                        check(mode);
                    }
                }
            }

            cgtfile.seek(0);
            //se il puntatore all'ultimo elemento è la fine del file allora ricalcolo il puntatore all'ultimo doc valido
            if (cgtfile.readLong() == cgtfile.length()) {
                //System.err.println("PASSO 5: Restoring the last key pointer (" +filepath +")");
                if (mode.matches(".*w.*")) {
                } else {
                    cgtfile.close();
                    cgtfile = new RandomAccessFile(filepath, "rw");

                }

                cgtfile.seek(startPosition);
                String key = null;
                long nextpos = 0;
                long currpos = startPosition;
                int size = 0;
                if (cgtfile.length() > cgtfile.getFilePointer()) {
                    while ((line = cgtfile.readUTF()) != null ) {

                        if (line.matches("^\\d+" + separator + ".+"+separator+"\\d+")) {
                            nextpos = currpos;
                            cgtfile.seek(cgtfile.getFilePointer() + Long.parseLong(line.substring(0,line.indexOf(separator))));
                            key = line;
                            size++;
                            //System.err.println(key);
                        }
                        currpos = cgtfile.getFilePointer();
                    }
                    cgtfile.seek(0);

                    if (key != null) {
                        System.err.println("# WARNING! Restoring the last key pointer." + nextpos);
                        cgtfile.writeLong(nextpos);
                    } else {
                        cgtfile.readLong();
                    }
                    cgtfile.writeInt(size);
                    cgtfile.close();

                    cgtfile = new RandomAccessFile(filepath, mode);
                }
            }

            cgtfile.seek(0);
            cgtfile.readLong();
            itemCounter = cgtfile.readInt();
            cgtfile.readLine();
            //System.err.println(startPosition +" -- " +cgtpath +"\n("+line+")");
            startPosition = cgtfile.getFilePointer();

            initialLen = cgtfile.length();

            String setpath;
            if (cgtpath.endsWith(".cgt"))
                setpath = cgtpath.replaceAll("\\.cgt$",".set");
            else
                setpath = cgtpath + ".set";

            File file2 = new File(setpath);
            if (!file2.exists()) {
                // System.err.println("# Restoring... " +file2);
                // int counterSetURL = 0;
                setfile = new SetFile(file2.getCanonicalPath());

                String[] sline;
                cgtfile.seek(startPosition);
                if (cgtfile.length() > cgtfile.getFilePointer()) {
                    long pos = cgtfile.getFilePointer();
                    while ((line = cgtfile.readUTF()) != null ) {
                        if (line.matches("^\\d+" + separator + ".+"+separator+"\\d+")) {
                            sline = line.split(separator);
                            if (sline.length > 3) {
                                if (line.matches("^\\d+" + separator + "\\+.+"+separator+"\\d+")) {
                                    setfile.put(sline[1].substring(1),String.valueOf(pos));
                                    // counterSetURL++;

                                    //get redirection
                                    int fieldPos = line.indexOf(separator+"redirect-from" + "=");
                                    if (fieldPos > 0) {
                                        line = line.substring(fieldPos + "redirect-from".length() +separator.length()+ 1,
                                                line.indexOf(separator,fieldPos+separator.length()));
                                        //System.err.println("@@@ " + line);
                                        setfile.put(line,String.valueOf(pos));
                                    }
                                }

                            } else {
                                System.err.println("> READING ERROR (position: "+ cgtfile.getFilePointer() +", file: "+cgt+ ")");
                            }
                            cgtfile.seek(cgtfile.getFilePointer() + Long.parseLong(sline[0]));

                        }
                        if (cgtfile.getFilePointer()>=cgtfile.length())
                            break;
                        pos = cgtfile.getFilePointer();
                        // System.err.println(counterSetURL);
                    }
                }
            } else {
                //System.err.println("PASSO 4");

                setfile = new SetFile(file2.getCanonicalPath());

                //System.err.println("Loaded " + this.size() + " (unique " + setfile.size() + ") text items ("+filepath+").");
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /** Return the archive file path
     * @return filepath
     */
    public String getFilepath() {
        try {
            return cgt.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    private boolean isWritable () throws IOException {
        return isWritable;
    }



    private String setHeader (String key, int datalen, Map header) {

        String info = separator +"+"+key;
        if (header != null && header.containsKey("dtime")) {
            info += separator + "dtime=" + header.get("dtime");
            header.remove("dtime");
        } else {
            info += separator + "dtime=" + new Date();
        }

        if (header != null && header.size() > 0) {
            Iterator it = header.entrySet().iterator();
            String item;
            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry)it.next();
                item = pairs.getKey() + "=" + pairs.getValue();

                if (!item.contains(separator)) {
                    info += separator + item.trim();
                } else {
                    info += separator + item.replaceAll(separator,"#_").trim();
                }
            }
            info = info.replaceAll("[\n|\r]+"," ");
        }
        return info + separator + datalen;
    }

    /** Writes a new key (typically an URL) and its value info (like the content of the html page) to the archive.
     * @param key, byte[] bytes
     */
    public boolean add(String key, byte[] bytes) {
        return writeItem(key,bytes, null);
    }

    /** Writes a new key and its text and header info (like a value in the hash hash) to the archive.
     * @param key, byte[] bytes, Map header
     */
    public boolean add(String key, byte[] bytes, Map header) {
        return writeItem(key, bytes, header);
    }

    /** Writes a new key (typically an URL) and its value info (like the content of the html page) to the archive.
     * @param key, String text
     */
    @Deprecated
    public boolean add(String key, String text) {
        return writeItem(key,text, null);
    }

    /** Writes a new key and its text and header info (like a value in the hash hash) to the archive.
     * @param key, String text, Map header
     */
    @Deprecated
    public boolean add(String key, String text, Map header) {
        return writeItem(key, text, header);
    }


    synchronized private boolean updateFileStat (long lastkeypos) throws IOException {
        System.err.println("initialLen updateFileStat -->");
        cgtfile.seek(0);
        itemCounter++;
        cgtfile.writeLong(lastkeypos);
        cgtfile.writeInt(itemCounter);
        initialLen = cgtfile.length();

        return true;
    }

    /** Writes a new key and its text and header info (like a value in the hash hash) to the archive.
     * @param key, String text, String hinfo
     */
    synchronized private boolean writeItem(String key, String text, Map head) {
        if (key != null && !key.contains(separator) && text != null) {
            String header = setHeader(key, text.length(), head);

            try {
                long lastkeypos = cgtfile.length();
                if (isUpdating()) {
                    System.err.println("WARNING!! The archive " + cgt + " already in use.");
                    System.exit(0);
                }

                //byte[] data = serialize(text.getBytes("UTF8"));
                byte[] data = serialize(text);

                cgtfile.seek(lastkeypos);
                setfile.put(key, String.valueOf(lastkeypos));

                if (head != null && head.containsKey("redirect-from"))
                    setfile.put((String) head.get("redirect-from"), String.valueOf(lastkeypos));

                //System.err.println(text);
                cgtfile.writeUTF(String.valueOf(data.length) + header);
                long lastpos = cgtfile.length() + data.length + "\n".length();
                cgtfile.write(data);
                cgtfile.writeBytes("\n");

                if (lastpos != cgtfile.length()) {
                    System.err.println("\n# ERROR! The file is disalligned on key " + key + "(pos: " +cgtfile.getFilePointer() + ")");
                }
                updateFileStat(lastkeypos);

                //rafile.writeBytes(String.valueOf(pos.intValue() + pos.toString().length()+1) + info);
                //rafile.writeBytes(text + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }


    /** Writes a new key and its text and header info (like a value in the hash hash) to the archive.
     * @param key, byte[] data, String hinfo
     */
    synchronized private boolean writeItem(String key, byte[] text, Map head) {
        if (key != null && !key.contains(separator) && text != null) {
            String header = setHeader(key, text.length, head);
            try {
                long lastkeypos = cgtfile.length();
                if (isUpdating()) {
                    System.err.println("WARNING! The archive " + cgt + " already in use.");
                    this.close();
                    System.exit(0);
                }
                byte[] data = gzip(text);

                cgtfile.seek(lastkeypos);
                setfile.put(key, String.valueOf(lastkeypos));
                if (head != null && head.containsKey("redirect-from"))
                    setfile.put((String) head.get("redirect-from"), String.valueOf(lastkeypos));

                //System.err.println(text);
                cgtfile.writeUTF(String.valueOf(data.length + 1) + header);

                long lastpos = cgtfile.getFilePointer() + data.length + 1;
                cgtfile.write(data);
                cgtfile.writeBytes("\n");

                if (lastpos != cgtfile.getFilePointer()) {
                    System.err.println("\n# ERROR!! The file is disalligned on key " + key + "(pos: " +cgtfile.getFilePointer() + ")");
                }
                updateFileStat(lastkeypos);

                //rafile.writeBytes(String.valueOf(pos.intValue() + pos.toString().length()+1) + info);
                //rafile.writeBytes(text + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    /*
    public void add(File file, Charset charset) {
        StringBuffer text = new StringBuffer();
        try{
            InputStreamReader in = new InputStreamReader(new FileInputStream(file),charset);
            BufferedReader filereader = new  BufferedReader(in);

            String line;
            while((line=filereader.readLine())!=null) {
                text.append(line).append("\n");
            }
            filereader.close();
            in.close();

            add(file.getName(),text.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void add(File file) {
        add(file,null);
    }

    public String getURL(String key) {
        if (texthash.containsKey(key)) {
            try {
                Long currpos = (Long) texthash.get(key);
                this.seek(currpos);
                String line = this.readLine();
                String[] sline = line.split(separator);

                if (sline.length > 1) {
                    //System.err.println(sline[1]);
                    return sline[1];
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }*/

    /** Gets the header field value of the key.
     * @param key, field
     * @return String
     */
    public String getHeader(String key, String field) {
        String value = null;
        try {
            String currpos = setfile.get(key);
            if (currpos != null ) {
                cgtfile.seek(Long.parseLong(currpos));
                String line = cgtfile.readUTF();
                int fieldPos = line.indexOf(separator+field + "=");
                if (fieldPos > 0) {
                    value = line.substring(fieldPos + field.length() +separator.length()+ 1,
                            line.indexOf(separator,fieldPos+separator.length()));
                    value = value.trim();
                }
                //System.err.println("> (" + value + ")");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return value;
    }


    /** Gets the header values as a map object about a specific key.
     * @param key
     * @return Map<field,value>
     */
    public LinkedHashMap<String, String> getHeader(String key) {
        LinkedHashMap<String, String> value = new LinkedHashMap<String, String>();
        try {
            String currpos = setfile.get(key);
            if (currpos != null ) {
                cgtfile.seek(Long.parseLong(currpos));
                value = parserHeader(cgtfile.readUTF());
                //System.err.println(key +" ==> "+ value);
                // value = parserHeader(cgtfile.readLine());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return value;
    }

    public LinkedHashMap<String, String> parserHeader(String line) throws UnsupportedEncodingException {
        LinkedHashMap<String,String> value = new LinkedHashMap<String,String>();
        if (line != null) {
            //System.out.println(line.substring(line.lastIndexOf(separator)) + " " +line);
            if (line.lastIndexOf(separator)> 0)
                value.put("pagesize", line.substring(line.lastIndexOf(separator) + separator.length()));
            String[] fields = line.split(separator);
            int equalpos;
            for (int f=2; f< fields.length; f++) {
                equalpos = fields[f].indexOf("=");
                if (equalpos > 1) {
                    value.put(fields[f].substring(0,equalpos),fields[f].substring(equalpos +1));

                }
            }
        }
        return value;
    }



    /** Gets the object value about a specific key.
     * @param key
     * @return Serializable
     */
    synchronized public Serializable get (String key) {
        String text = null;

        try {
            synchronized ( this ) {
                String currpos =  setfile.get(key);
                if (currpos != null ) {
                    cgtfile.seek(Long.parseLong(currpos));
                    //String line = cgtfile.readLine();
                    String line = cgtfile.readUTF();
                    /*String encoding = "UTF8";
                   int encpos = line.indexOf("#:encoding=");
                   if (encpos != -1) {
                       encpos +=11;
                       encoding = line.substring(encpos,line.indexOf(separator,encpos));
                   } */
                    //System.err.println(encoding +"> " + line);

                    if (line != null && line.matches("^\\d+"+separator+ "\\-.+"+separator+"\\d+")) {
                        System.err.println("# WARNING! The entry "+key+ " has been marked as REMOVED.");
                    } else if (line != null && line.matches("^\\d+"+separator+ "\\+.+"+separator+"\\d+")) {
                        int datalen = Integer.parseInt(line.substring(0,line.indexOf(separator)));
                        //long datalen = Long.parseLong(line.substring(line.lastIndexOf(separator) + separator.length()));
                        //System.err.println("#" + (nextpos - line.length() - currpos.intValue() -1 ) + " < " + this.length());

                        //byte[] b = (byte[]) readObject(this.getFilePointer(), (nextpos - line.length() - currpos.intValue() -1));
                        text = (String) deserialize(cgtfile.getFilePointer(), datalen);
                        //text = new String(b,encoding);

                        if (errorno == 0 && text.length() != datalen) {
                            System.err.println("# WARNING! Archive is corrupted for entry: " + key+ ". Lenght of data is " + text.length() + " != " + datalen+ ".");
                            errorno++;
                        }
                    } else {
                        System.err.println("# WARNING! Archive is corrupted for entry: " + key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return text;
    }

    /** Gets the object value about a specific key.
     * @param key
     * @return Serializable
     */
    synchronized public String getEncodedText(String key) throws IOException {
        return getEncodedText(key,null);
    }

    synchronized public String getEncodedText(String key,String encoding) {
        try {
            synchronized ( this ) {
                if (encoding == null) {
                    encoding = getEncoding(key);
                }

                byte[] content = getBytes(key);
                if (content != null) {
                    if (encoding != null && encoding.length() > 0) {
                        return new String(content, encoding);
                    } else {
                        return new String(content);
                    }

                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }

    synchronized public byte[] getBytes (String key) {
        if (key == null || key.length() == 0)
            return null;
        byte[] text = null;
        String line = "";
        try {
            synchronized ( this ) {
                String currpos = setfile.get(key);
                //System.err.println(currpos + ">: "+key);
                if (currpos != null ) {
                    cgtfile.seek(Long.parseLong(currpos));
                    //TODO
                    //line = cgtfile.readLine();
                    line = cgtfile.readUTF();
                    //System.err.println("LINE: "+line);
                    /*String encoding = "UTF8";
                   int encpos = line.indexOf("#:encoding=");
                   if (encpos != -1) {
                       encpos +=11;
                       encoding = line.substring(encpos,line.indexOf(separator,encpos));
                   } */


                    if (line != null && line.matches("^\\d+"+separator+ ".+"+separator+"\\d+")) {
                        String nextposition = line.substring(0,line.indexOf(separator));
                        text = gunzip(cgtfile.getFilePointer(),Integer.parseInt(nextposition));
                        if (text == null) {
                            System.err.println("# WARNING!! Archive is corrupted for entry "+key);
                        } else {
                            String dlen = line.substring(line.lastIndexOf(separator) + separator.length()).replaceFirst("\n","");
                            long datalen = -1;
                            if (dlen.matches("^\\d+$"))
                                datalen = Long.parseLong(dlen);

                            if (errorno == 0 && text.length != datalen) {
                                System.err.println("# WARNING!!! Archive is corrupted for entry "+key+". Lenght of data is " + text.length + " != " + datalen);
                                errorno++;
                            }
                        }
                    } else {
                        System.err.println("# WARNING!!! Archive is corrupted for key "+key+".");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return text;
    }

    public String getUTF8Text (String key) throws UnsupportedEncodingException {
        byte[] bb = getBytes(key);
        //String encoding = getEncoding(key);

        if (bb != null)
            return new String(bb, "UTF8");
        return "";
    }

    public String getEncoding (String key) {
        String encoding= getHeader(key, "encoding");
        try {
            synchronized ( this ) {
                //todo: guess encoding
                String contentType = getHeader(key, "content-type");
                if (contentType != null && contentType.contains("charset=")) {
                    encoding = contentType.replaceFirst("^.*charset=","").trim();
                }
                //if (encoding == null || encoding.equals(""))
                //    encoding = HtmlFile.guessEncoding(getBytes(key));
            }
        } catch (Exception e) {
            //System.err.println("Encoding error: " +e.getMessage());
            //anche se non ho trovato un encoding provo a codificare Latin-1 per default
            // anche perché il guesser é molto preciso nel caso di UTF-8
            return "ISO-8859-1";
        }
        return encoding;
    }

    //parser time like: Sat Oct 11 01:30:53 CEST 2008
    public Date getTime (String url) {
        String time = getHeader(url,"dtime");
        if (time != null)
            try {
                return formatter.parse(time);
            } catch (ParseException e) {
                SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ITALIAN);
                try {
                    return formatter.parse(time);
                } catch (ParseException e2) {
                    System.err.println("# ERROR " + e2.getMessage());
                }
            }
        return null;
    }

    public String getUrl (String key) {
        try {
            String currpos = setfile.get(key);
            if (currpos != null) {
                cgtfile.seek(Long.parseLong(currpos));
                String line = cgtfile.readUTF();

                if (line.matches("^\\d+" + separator + ".+"+separator+"\\d+")) {
                    String[] sline = line.split(separator);
                    if (sline.length > 3)
                        return sline[1];
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /** Returns true if there is another key
     * @return boolean
     */
    private boolean hasMoreKeys() {
        try {
            //System.err.println("hasMoreKeys(): currPosition=" + currPosition + " lenght=" + this.length());
            if (nextPosition >= cgtfile.length()) {
                return false;
            }
            cgtfile.seek(nextPosition);
            String line = cgtfile.readUTF();
            if (line == null) {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /** Returns the first stored key
     * @return String
     */
    public String firstKey() {
        nextPosition = startPosition;
        return nextKey();
    }


    /** Returns the last stored key
     * @return String
     */
    public String lastKey() {
        try {
            cgtfile.seek(0);
            cgtfile.seek(cgtfile.readLong());
            nextPosition = cgtfile.length();

            String line = cgtfile.readUTF();
            if (line != null) {
                int separPos = line.indexOf(separator);

                return line.substring(separPos+separator.length()+1,line.indexOf(separator,separPos+separator.length()+1));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String nextKey (String key) {
        String currpos =  setfile.get(key);
        if (currpos != null) {
            nextPosition = Long.parseLong(currpos);
            nextKey();
            return nextKey();
        }


        return null;

    }

    /** Returns the next stored key
     * @return String
     */
    public boolean hasNext() {
        try {
            if (nextPosition < cgtfile.length()) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /** Returns the next stored key
     * @return String
     */
    public String nextKey() {
        try {
            if (nextPosition < cgtfile.length()) {
                cgtfile.seek(nextPosition);

                if (cgtfile.getFilePointer() < cgtfile.length()) {
                    String line;
                    while ((line = cgtfile.readUTF()) != null) {
                        //if (line.matches(".*\\d+"+separator+ ".+"+separator+"\\d+")) {
                        int separPos = line.indexOf(separator);
                        nextPosition = cgtfile.getFilePointer() + Long.parseLong(line.substring(0,separPos));
                        //System.out.println(separPos + " " +nextPosition +" LINE: " +line);

                       /*
                        if (line.contains("/9901046.xml")) {
                                                    cgtfile.seek(nextPosition);
                            line = cgtfile.readUTF();
                            String info = line.substring(separPos+separator.length()+1,line.indexOf(separator,separPos+separator.length()+1));
                            System.out.println(separPos+" :: (" +info+ ") >>> " + line.matches("#:\\+") + " --- " +line);

                            nextPosition = cgtfile.getFilePointer() + Long.parseLong(line.substring(0,separPos));
                            System.out.println("nextPosition: " + nextPosition);
                            cgtfile.seek(nextPosition);
                            line = cgtfile.readUTF();
                            System.out.println("## "+line);
                            System.exit(0);
                        }
                           */

                        if (separPos>0) {
                            return line.substring(separPos+separator.length()+1,line.indexOf(separator,separPos+separator.length()+1));
                        } else {
                            cgtfile.seek(nextPosition);
                            //}
                        /*else {
                                System.err.print ("\n# ERROR on " +nextPosition+" " + cgtfile.getFilePointer() + ": " +line);
                                cgtfile.seek(nextPosition-10);
                                //System.err.print (" LINE0 (" +cgtfile.readUTF() +")");

                                //System.err.println (" LINE1 (" +cgtfile.getFilePointer() +") " + cgtfile.readUTF());

                                while (line != null) {
                                    if (line.matches("^\\d+"+separator + ".+"+separator+"\\d+")) {
                                        sline = line.split(separator);
                                        //System.err.println("! " +cgtfile.getFilePointer() + " " + line);
                                        nextPosition = cgtfile.getFilePointer() + Long.parseLong(sline[0]);
                                        return sline[1];
                                    }
                                    line = cgtfile.readUTF();
                                }
                            }
                            */
                            if (cgtfile.getFilePointer() >= cgtfile.length())
                                break;
                        }
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /** Returns an iterator of the keys in this archive.
     * @return  Iterator
     */
    public Iterator iterator() {
        return setfile.iterator();
    }


    public int getIndexedDocSize() {
        return setfile.size();
    }

    /** Returns the number of stored elements
     *
     * @return int
     */
    public int size() {
        try {
            cgtfile.seek(0);
            cgtfile.readLong();
            return cgtfile.readInt();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
        //return setfile.size();
    }

    /** Checks if the key is already stored
     * @param key
     * @return boolean
     */
    public boolean contains(String key) {
        String currpos = setfile.get(key);
        if (currpos != null ) {
            try {
                cgtfile.seek(Long.parseLong(currpos));
                String line = cgtfile.readUTF();
                if (line.indexOf(separator+"+"+key+separator) > 0) {
                    return true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /** mark a key as remove
     *
     * @param key
     * @return
     */
    private boolean remove (String key) {
        String currpos = setfile.get(key);
        if (currpos != null ) {
            try {
                cgtfile.seek(Long.parseLong(currpos));
                String line = cgtfile.readUTF();
                if (line.matches(".*\\d+"+separator+"\\+")) {
                    int pos =line.indexOf(separator);
                    line = line.substring(0,pos)+separator+line.substring(pos+1);
                    cgtfile.seek(Long.parseLong(currpos));
                    cgtfile.writeUTF(line);
                    return true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return false;
    }

    public void close() {
        try {
            cgtfile.close();
            setfile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***
     * Returns the serialized form of the given object in a byte array.
     *
     * @return byte[]
     * @param obj
     * @exception IOException
     */

    private byte[] serialize( Serializable obj ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(baos));
        try {
            oos.writeObject(obj);
        } finally {
            baos.close();
            oos.close();
        }
        return baos.toByteArray();
    }


    /***
     * Returns the deserialized object given a byte array.
     *
     * @return Object
     * @param len
     * @exception IOException
     */

    private Object deserialize(long from, int len) throws IOException {
        byte[] data = readObject(from,len);
        ObjectInputStream ois = new ObjectInputStream(
                new GZIPInputStream(
                        new ByteArrayInputStream(data))
        );

        try {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            ois.close();

        }

        return null;
    }

    /***
     * This reads an byte[] from the given starting position on the file.
     * <p>
     * The first four bytes of the record should tell us how long it is. The data
     * is read into a byte array and then an object is constructed from the byte
     * array.
     *
     * @return byte[]
     * @param pos, len
     */

    private byte[] readObject(long pos, int datalen) {
        byte[] data = new byte[datalen];
        boolean corrupted = false;

        try {
            synchronized ( this ) {
                if ( pos > cgtfile.length()) {
                    corrupted = true;
                    //System.err.println("Position is greater than file length");
                } else if (cgtfile.length() == 0) {
                    return null;
                } else {
                    cgtfile.seek( pos );
                    //System.err.println("pos: " + pos + " -- " + this.size() + " >" +this.length());
                    if ( datalen > cgtfile.length() ) {
                        corrupted = true;
                        //System.err.println("Position (" + pos + ") + datalen (" + datalen + ") is greater than file length");
                    } else {
                        cgtfile.read(data);
                        //System.out.println("Position (" + pos + ") + datalen (" + datalen + ")");

                    }
                }
            }

            if ( corrupted ) {
                //reset();
                throw new IOException( "ERROR! The index file is corrupted." );
                // return null;
            }
            return data;
            //return (Serializable) deserialize(data);
            //return gunzip(data);
        }
        catch ( Exception e ) {
            if ( e instanceof IOException ) {
                System.err.println("# " + e);
            }
        }
        return null;
    }


    /***
     * Returns the gzip form of the given object in a byte array.
     *
     * @return byte[]
     * @param data
     * @exception IOException
     */

    private byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream oos = new GZIPOutputStream(baos);

        try {
            oos.write(data);
            oos.flush();
            oos.close();
        } finally {
            baos.close();
        }
        return baos.toByteArray();
    }


    /***
     * Returns the gzip form of the given object in a byte array.
     *
     * @return byte[]
     * @param len
     */

    private byte[] gunzip(long from, int len) {
        byte[] result = null;


        ByteArrayOutputStream bas = null;
        ByteArrayInputStream baos =null;
        GZIPInputStream oos=null;
        try {
            byte[] data = readObject(from, len);

            bas = new ByteArrayOutputStream();

            baos = new ByteArrayInputStream(data);
            oos = new GZIPInputStream(baos);

            byte[] tmpBuffer = new byte[4096];
            int n;
            while ((n = oos.read(tmpBuffer)) > 0) {
                bas.write(tmpBuffer,0,n);
                // System.err.println("::"+n + " " + tmpBuffer.length + " size=" +bas.size()+" ("+tmpBuffer+")");

            }
            //System.err.println(new String(bas.toByteArray()) + "\n" +tot);
            result = bas.toByteArray();

            //String tmp = new String(result);
            //System.err.println(tmp + "\n" + tmp.length());
            oos.close();
            bas.close();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    //read the cgtfile and create an bytes archive
    synchronized private void Object2Bytes2 (CatalogOfGzippedTexts newcgt) {
        try {
            cgtfile.seek(0);

            String line = null;
            int separPos = 0;
            Map header;
            byte[] data;
            while ((line = cgtfile.readLine()) != null) {
                if (line.matches(".*\\d+"+separator+ ".+"+separator+"\\d+")) {
                    header = parserHeader(line);
                    separPos = line.indexOf(separator);
                    String nextposition = line.substring(0,separPos);
                    nextPosition = cgtfile.getFilePointer() + Long.parseLong(nextposition);

                    data = gunzip(cgtfile.getFilePointer(),Integer.parseInt(nextposition));

                    newcgt.add(line.substring(separPos+separator.length(),line.indexOf(separator,separPos+separator.length())),data,header);

                    cgtfile.seek(nextPosition);
                }
                System.err.println("!! "+line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //read the cgtfile and create an bytes archive
    private void Object2Bytes (CatalogOfGzippedTexts newcgt) {
        try {
            Map header;
            String line;
            String[] sline;
            long position = 0;
            cgtfile.seek(position);
            while ((line = cgtfile.readUTF()) != null ) {
                sline = line.split(separator);


                if (sline.length > 1) {
                    String encoding = "";
                    int encpos = line.indexOf("#:encoding=");
                    if (encpos != -1) {
                        encpos+=11;
                        encoding = line.substring(encpos,line.indexOf(separator,encpos));
                    }

                    //System.err.println(sline[1] + " -- " + this.getFilepath());
                    int nextpos = Integer.parseInt(sline[0]);

                    String text = (String) deserialize(cgtfile.getFilePointer(),nextpos - line.length() - (int) position -1);

                    header = parserHeader(line);

                    if (encoding.length() == 0) {
                        newcgt.add(sline[1],text.getBytes(),header);
                    } else {
                        newcgt.add(sline[1],text.getBytes(encoding),header);
                    }

                    //System.out.println(++i + "\t" +encoding + "\t" + sline[1]);

                } else {
                    System.err.println("> READING ERROR (position "+ position +"): " + line);
                    System.exit(-1);
                }
                position = Long.parseLong(sline[0]);
                cgtfile.seek(position);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    synchronized private void check (String mode) {
        try {
            Pattern pattern = Pattern.compile("[0-9]+$");
            String cgtpath = cgt.getCanonicalPath();
            File newfile = new File(cgtpath.replaceAll("\\.cgt$","_temp.cgt"));

            if (newfile.exists() && !newfile.delete()) {
                System.err.println("Before to continue you must delete the file " +newfile);
                return;
            }
            String b;
            String[] items;

            //la versione 1.1 prevedeva di avere sulla prima riga il prima header in cui il primo numero era la posizione
            // assoluta al prosimo header. Dalla versione 1.2 abbiamo sulla prima linea le informazioni sull'archivio
            // (l'ultimo bytes registrato, il numero tot. di documenti, ...) e il primo numero dell'header è la posizione relativa
            // al prossimo documento
            //
            initialLen = cgtfile.length();
            cgtfile.seek(0);
            b = cgtfile.readLine();
            boolean ver1_2 = true;
            if (b.matches(".*\\d+"+separator+".+"+ separator+"\\d+")) {
                ver1_2 = false;
                cgtfile.seek(0);
            }

            //System.err.println(   cgt.getFilePointer() +" -- " + cgt.length());
            System.err.println("# Archive " + cgtpath + " ...UPDATING");
            CatalogOfGzippedTexts cgtnew = new CatalogOfGzippedTexts(newfile.getCanonicalPath(),"rw");

            long currpos;
            String key = "";
            Matcher matchnextpos;
            Map<String,String> header = new HashMap<String,String>();
            byte[] text;
            while((b = cgtfile.readLine()) != null) {
                items = b.split(separator);
                if (items.length >= 2) {
                    if (b.matches(".*\\d+"+separator+".+" + separator+"\\d+")) {
                        currpos = cgtfile.getFilePointer();
                        //System.err.println(cgtfile.getFilePointer() +" -- " + b);
                        try {
                            int firstitem=0;
                            boolean foundheader = false;
                            for (int i=0; i<items.length; i++) {
                                if (items[i].startsWith("dtime=")) {
                                    firstitem=i-2;
                                    foundheader=true;
                                    break;
                                }
                            }
                            if (!foundheader)
                                continue;

                            key = items[firstitem+1];
                            //if (nullcounter > 0)
                            //  System.err.println("! " + key);
                            if (!items[firstitem].matches("^\\d+$")) {
                                matchnextpos = pattern.matcher(items[firstitem]);
                                if (matchnextpos.find()) {
                                    items[firstitem] = matchnextpos.group(0);
                                    /*if (!items[firstitem].matches("^\\d+$")) {
                                        System.err.println("% " + items[firstitem] + " --> " + items[firstitem+1] );
                                    } else {
                                        System.err.println("|| " + items[firstitem]);
                                    }*/
                                }
                            }


                            if (ver1_2)
                                text = gunzip(currpos ,(int) (Long.parseLong(items[firstitem])));
                            else
                                text = gunzip(currpos ,(int) (Long.parseLong(items[firstitem]) - currpos));

                            if (text == null) {
                                cgtfile.seek(currpos);
                                System.err.println("ERROR on " +cgtpath +"! Archive is corrupted for key "+key);

                            } else {

                                header.clear();
                                for (int i=firstitem+2; i<items.length; i++) {
                                    if (items[i].contains("=")) {
                                        //System.err.println(items[i].substring(0,items[i].indexOf("=")));
                                        header.put(items[i].substring(0,items[i].indexOf("=")), items[i].substring(items[i].indexOf("=")+1));
                                    }
                                }
                                //System.err.print(b.substring(b.indexOf(separator)));
                                System.err.println("Updating... "+ key + " (" +text.length+ " chars)");
                                //if (!cgtnew.contains(key)) {
                                cgtnew.add(key,text,header);
                                //}
                            }
                        } catch (Exception e) {
                            System.err.println("ERROR! Archive is corrupted for key "+key);
                            e.printStackTrace();

                        }
                    }

                }

            }

            //se il cgt non è stato modificato allora provvedo alla sostituzione (altrimenti perderei dei dati)
            File newsetfile = new File(cgtpath.replaceAll("\\.cgt$","_temp.set"));
            if (!isUpdating()) {
                cgtfile.close();
                cgtnew.close();

                //move the start file
                newfile.renameTo(new File(cgtpath));

                File setfile = new File(cgtpath.replaceAll("\\.cgt$",".set"));
                newsetfile.renameTo(setfile);

                cgtfile = new RandomAccessFile(cgtpath, mode);
            } else {
                newfile.delete();
                newsetfile.delete();
                System.err.println("ERROR! Updating cgt file to new version has been failed. File " + cgtpath + " is growing up.");

                System.exit(0);
            }

        } catch (Exception e) {
            System.err.println("ERROR! Checking is not done correctly.");
            e.printStackTrace();
        }


    }

    public boolean isUpdating () throws IOException {
        System.err.println("isUpdating... initialLen: " + initialLen + " != cgtfile.length: " + cgtfile.length());
        if (initialLen != cgtfile.length()) {
            return true;
        }
        return false;
    }

    private String utc2cgt (String newcgtpath) {
        try {
            CatalogOfGzippedTexts newcgt = new CatalogOfGzippedTexts (newcgtpath,"rw");
            Object2Bytes(newcgt);
            newcgt.close();

            return newcgt.cgt.getCanonicalPath();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main (String args[]) throws Exception {
        CatalogOfGzippedTexts cgt = null;
        try {
            cgt = new CatalogOfGzippedTexts (args[0],"r");
            System.err.println("Loaded " + cgt.size() + " stored texts.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        if (args.length == 2) {
            if (cgt != null)
                System.err.println("The archive " + cgt.utc2cgt(args[1]) + " has been fixed!");

        } else {
            /*Enumeration enumurls = cgt.keys();
            while(enumurls.hasMoreElements()) {
                System.out.println(enumurls.nextElement());
            } */

            if (cgt != null)
                System.err.println("The archive " + cgt.getFilepath() + " contains " + cgt.size() +" texts.");

        }

    }
}
