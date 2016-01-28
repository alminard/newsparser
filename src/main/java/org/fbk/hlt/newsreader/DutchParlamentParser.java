package main.java.org.fbk.hlt.newsreader;

import fbk.hlt.utility.archive.CatalogOfGzippedTexts;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by IntelliJ IDEA.
 * User: cgirardi
 * Date: 10-August-2014
 * Time: 1.19.07
 */
public class DutchParlamentParser {
    private DefaultHandler handler;
    private SAXParser saxParser;
    private String newsUrl = "";
    private String zipPath;
    static private Hashtable elementStat = new Hashtable();
    static private Hashtable elementExample = new Hashtable();
    static private CatalogOfGzippedTexts cgtarchive;
    static private int counterSkipped = 0;

    public DutchParlamentParser(String zipPath) {
        this.zipPath = zipPath;

        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(false);


        try {
            //factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            saxParser = factory.newSAXParser();

            handler = new DefaultHandler() {
                String relevantElement = null;
                StringBuilder body = new StringBuilder();
                StringBuilder content = new StringBuilder();
                LinkedHashMap<String, String> header = new LinkedHashMap<String, String>();
                private int counter = 0;
                static final int MINIMAL_BODY_LEN = 100;
                public void startElement(String uri, String localName,String qName,
                                         Attributes attributes) throws SAXException {
                    //element monitor
                    if (!elementStat.containsKey(qName)) {
                        elementStat.put(qName, new Integer(1));
                    } else {
                        elementStat.put(qName, (Integer) elementStat.get(qName) +1);
                    }
                    if (!elementExample.containsKey(qName) && attributes.getLength() > 0) {
                        elementExample.put(qName, attributes.toString());
                    }

                    if (qName.equalsIgnoreCase("document")) {
                        body.setLength(0);
                        header.clear();
                    }
                    if (qName.equalsIgnoreCase("hiddendatum") ||
                            qName.equalsIgnoreCase("titel") ||
                            qName.equalsIgnoreCase("trefwoord") ||
                            qName.equalsIgnoreCase("tekstxml") ||
                            qName.equalsIgnoreCase("tekstantwoord") ||
                            qName.equalsIgnoreCase("abstractxml")) {
                        relevantElement = qName;
                    }

                    /*if (qName.equalsIgnoreCase("date.issue")) {
                        String date = attributes.getValue("norm");
                        if (date != null)
                            date = date.substring(0,8);
                        header.put("date", date);
                    }*/

                }

                public void endElement(String uri, String localName, String qName) throws SAXException {
                    if (!elementExample.containsKey(qName) && content.toString().trim().length()>0) {
                        elementExample.put(qName, content.toString());
                    }

                    if (qName.equalsIgnoreCase("hiddendatum")) {
                        if (content.toString().matches("\\d\\d\\d\\d\\.\\d\\d\\.\\d\\d")) {
                            header.put("date", content.toString().replaceAll("\\.","-")+"T00:00:00");
                        }
                    } else if (qName.equalsIgnoreCase("trefwoord")) {
                        if (content.length() > 0) {
                            if (header.containsKey("topic")) {
                                header.put("topic", header.get("topic") +", "+ content.toString());
                            } else {
                                header.put("topic", content.toString());
                            }
                        }
                    } else if (qName.equalsIgnoreCase("titel")) {
                        header.put("title", content.toString().trim());
                    } else if (qName.equalsIgnoreCase("abstractxml")) {
                        //header.put("abstract", content.toString());
                        body.append(content.toString()).append("\n\n");
                    } else if (qName.equalsIgnoreCase("tekstxml") || qName.equalsIgnoreCase("tekstantwoord")) {
                        body.append(content.toString()).append("\n");
                    } else if (qName.equalsIgnoreCase("document")) {
                        content.setLength(0);
                        String[] lines = body.toString().split("\n");
                        boolean addspace = false;
                        for (String line : lines) {
                            if (line.startsWith("<p>") && line.endsWith("</p>")) {
                                line = line.substring(3,line.indexOf("</p>")).trim();
                            }
                            if (line.endsWith("<br />")) {
                                line = line.substring(0,line.length()-6);
                                line = line.replaceAll("^</*[p|P]>","\n").replaceAll("</*[^>]+>","").trim();
                                int upperCount = line.split("[A-Z]").length - 1;

                                if ((line.startsWith("---") && line.endsWith("---")) ||
                                    upperCount > line.length() * 0.7 ||
                                        (line.matches("^[0-9]+.*") && line.length() < 10)) {
                                    content.append("\n").append(line).append("\n");
                                    addspace=false;
                                } else {
                                    if (addspace)
                                        content.append(" ");
                                    content.append(line);
                                    addspace=true;
                                }
                            } else {
                                if (content.toString().endsWith(",") ||
                                        content.toString().endsWith(".")) {
                                    content.append(" ").append(line);
                                } else {
                                    content.append(line).append("\n");
                                }
                                addspace=false;
                            }
                        }

                        if (content.toString().length() > MINIMAL_BODY_LEN) {
                            String raw = "";
                            if (header.containsKey("title")) {
                                raw = ((String) header.get("title"))+"\n\n";
                            }
                            raw += content.toString().replaceAll("</*[p|P]>","\n").replaceAll("<br />","\n").replaceAll("</*[^>]+>","").trim();

                            System.err.print("# " + newsUrl+", DATE: " +header.get("date") + ", LENGTH: " +raw.length() +"\n" + raw);

                            header.put("encoding", "UTF8");
                            header.put("filename", newsUrl);
                            header.put("id",newsUrl.replaceAll(".*/","").replaceFirst("\\.xml$",""));

                            // store the metadata as Map and the body into the cgt archive
                            String xml = null;
                            try {
                                xml = ZipXML2NAF.getXML(newsUrl,
                                        raw,
                                        header, "nl");
                                //System.err.println(xml);
                                if (xml!=null && DutchParlamentParser.storeNews(newsUrl,
                                        xml,
                                        header)) {
                                    counter++;
                                    System.err.println(" ...STORED ["+ counter +"]");
                                } else {
                                    DutchParlamentParser.counterSkipped++;
                                    System.err.println(" ...SKIPPED!");
                                }
                            } catch (ParserConfigurationException e) {
                                e.printStackTrace();
                            }

                        } else {
                            DutchParlamentParser.counterSkipped++;
                            System.err.println(" ...SKIPPED!");
                        }
                    }
                    relevantElement = null;
                    content.setLength(0);
                }

                public void characters(char ch[], int start, int length) throws SAXException {
                    if (relevantElement != null) {
                        //append the interested text to the content
                        content.append(new String(ch, start, length));
                    }

                }

            };

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }


    }

    private static boolean storeNews (String url, String news, Map header) {
        if (cgtarchive != null && !cgtarchive.contains(url)) {
            return cgtarchive.add(url, news.getBytes(), header);
        }
        return false;
    }

    public void zip2cgt (String cgtPath) throws IOException {
        try {
            File archiveFile = new File(cgtPath);
            if (archiveFile.getParentFile() != null && !archiveFile.getParentFile().exists())
                archiveFile.getParentFile().mkdirs();
            cgtarchive = new CatalogOfGzippedTexts(cgtPath, "rw");
        } catch (Exception e) {
            e.printStackTrace();
        }

        ZipFile zf = new ZipFile(zipPath);
        try {
            for (Enumeration<? extends ZipEntry> e = zf.entries(); e.hasMoreElements();) {
                ZipEntry ze = e.nextElement();
                newsUrl = ze.getName();
                if (newsUrl.endsWith(".xml")) {
                    try {
                        saxParser.parse(zf.getInputStream(ze), handler);

                    } catch (SAXException e2) {
                        System.err.println("ERROR on " + newsUrl + " " +e2.getMessage());
                    }
                }
            }

        } finally {
            zf.close();
        }
    }


    public static void main (String args[]) throws Exception {
        DutchParlamentParser zip = new DutchParlamentParser(args[0]);
        zip.zip2cgt(args[1]);
        Set keys =elementStat.keySet();
        for (Iterator e = keys.iterator(); e.hasNext();) {
            String elname = (String) e.next();
            String ex = (String) elementExample.get(elname);
            if (ex != null) {
                System.err.print("- "+elname + " (" + elementStat.get(elname) + ") ");
                if (ex.length() > 60) {
                    ex = " -- " +ex.replaceAll("[\n|\r]+"," ").substring(0,59) + "...";
                }
                System.err.println(ex);
            }
        }
        System.err.println("The archive " + args[1] +" contains " +cgtarchive.size() +
                " documents (skipped "+DutchParlamentParser.counterSkipped+ " documents.)");

    }

}


