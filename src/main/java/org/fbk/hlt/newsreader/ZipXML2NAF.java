package main.java.org.fbk.hlt.newsreader;
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

import eu.fbk.nwrtools.URICleaner;
import org.jdom.CDATA;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Author: Christian Girardi (cgirardi@fbk.eu)
 * Date: 27-mag-20134
 *
 * Dato un file zip con le news di LN crea una zip con i NAF input file.
 *
 group 0: less than 100
 group 1: between 100 - 1000
 group 2: between 1000 - 2000
 group 3: between 2000 - 3000

 */

public class ZipXML2NAF {
    private DefaultHandler handler;
    private SAXParser saxParser;
    private String newsUrl = "";
    private String zipPath;
    private ZipOutputStream zipout = null;

    static private int MINSIZE = -1 , MAXSIZE = -1;
    static private String uriPrefix="http://www.newsreader-project.eu/data/";

    public ZipXML2NAF(String zipPath) {
        this.zipPath = zipPath;

        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setValidating(false);
        factory.setNamespaceAware(false);


        try {
            //factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            saxParser = factory.newSAXParser();

            handler = new DefaultHandler() {

                boolean relevantBlock = false;
                boolean relevantElement = false;
                StringBuilder body = new StringBuilder();
                LinkedHashMap header = new LinkedHashMap();
                StringBuilder currentContent = new StringBuilder();
                private int counter = 0;

                public void startElement(String uri, String localName,String qName,
                                         Attributes attributes) throws SAXException {
                    if (qName.equalsIgnoreCase("block") || qName.equalsIgnoreCase("hedline")) {
                        relevantBlock = true;
                    } else if (qName.equalsIgnoreCase("p") || qName.equalsIgnoreCase("hl1")) {
                        relevantElement = true;
                    } else if (qName.equalsIgnoreCase("pubdata")) {
                        String sect = attributes.getValue("position.section");
                        if (sect != null) {
                            //cleaning some strange characters: " " seems a ^M of Windows
                            sect = sect.replaceAll(" "," ");
                            header.put("section", sect);
                        }
                        String mag = attributes.getValue("name");
                        if (mag != null) {
                            //cleaning some strange characters: " " seems a ^M of Windows
                            mag = mag.replaceAll(" "," ");
                            header.put("magazine", mag);
                        }
                    } else if (qName.equalsIgnoreCase("doc.copyright")) {
                        String publ = attributes.getValue("holder");
                        if (publ != null) {
                            //cleaning some strange characters: " " seems a ^M of Windows
                            publ = publ.replaceAll(" "," ");
                            header.put("publisher", publ);
                        }

                    } else if (qName.equalsIgnoreCase("date.issue")) {

                        String date = attributes.getValue("norm");
                        if (date != null && date.length() >= 8) {
                            date = date.substring(0,8);
                            header.put("date", date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8)+"T00:00:00");
                        }
                    } else if (qName.equalsIgnoreCase("doc-id")) {
                        String id = attributes.getValue("id-string");
                        if (id == null)
                            id = java.util.UUID.randomUUID().toString();
                        header.put("id", id);
                    }
                    currentContent.setLength(0);
                }


                public void endElement(String uri, String localName, String qName) throws SAXException {

                    if (qName.equalsIgnoreCase("block")) {
                        relevantBlock = false;
                        body.append("\n");
                    } else if (qName.equalsIgnoreCase("hedline")) {
                        relevantBlock = false;
                        body.append("\n\n");
                    } else if (qName.equalsIgnoreCase("p") || qName.equalsIgnoreCase("hl1")) {
                        relevantElement = false;
                        if (qName.equalsIgnoreCase("hl1")) {
                            String title = body.toString().trim().replaceAll(";?\\s*[\r|\n].*","").trim();

                            if (title != null) {
                                //cleaning some strange characters: " " seems a ^M of Windows
                                title = title.replaceAll(" "," ");
                            }
                            header.put("title", title);
                            body.setLength(0);
                            body.append(title + "\n");
                        }
                        body.append("\n");

                    } else if (qName.equalsIgnoreCase("br")) {
                        body.append("\n");
                    } else if (qName.equalsIgnoreCase("byline")) {
                        header.put("author", currentContent.toString());
                    } else if (qName.equalsIgnoreCase("dateline")) {
                        header.put("location", currentContent.toString());
                    } else if (qName.equalsIgnoreCase("body")) {
                        if (body.length() > 0) {
                            System.err.print("> " + newsUrl);
                            header.put("encoding", "UTF8");

                            // store the news in the zip
                            if (storeNews(newsUrl, body.toString(), header, "en")) {
                                counter++;
                                System.err.println(" ...STORED ["+ counter +", "+body.toString().length() +" chars]");
                            } else {
                                System.err.println(" ["+body.toString().length() +" chars] ...SKIPPED!");
                            }
                            body.setLength(0);
                            header.clear();
                        }

                    }
                }

                public void characters(char ch[], int start, int length) throws SAXException {
                    if (relevantBlock && relevantElement) {
                        //append the interested text to the content
                        body.append(new String(ch, start, length));
                    }
                    currentContent.append(new String(ch, start, length));
                }

            };

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }


    }

    private boolean storeNews (String url, String news, Map header, String lang) {
        if (url != null && news != null) {
            String nafnews = null;
            try {
                //some restrictions
                if (news.replaceAll("[\r|\n]"," ").matches("^.*[a-z]+.*$")) {
                    if (MINSIZE != -1 && news.length() < MINSIZE) {
                        System.err.print(" (MINSIZE DOESN'T REACHED) ");

                        return false;
                    }
                    if (MAXSIZE != -1 && news.length() > MAXSIZE) {
                        System.err.print(" (MAXSIZE EXCEED)");

                        return false;
                    }
                } else {
                    System.err.print(" (NO LOWCASE LETTERS FOUND) ");
                    return false;

                }
                if (header.containsKey("title") && news.trim().equalsIgnoreCase((String) header.get("title"))) {
                    System.err.print(" (only title) ");
                    return false;
                }

                //cleaning some strange characters: " " seems a ^M of Windows
                news = news.replaceAll(" "," ");

                nafnews = getXML(url, news, header, lang);
                if (nafnews != null) {
                    try {
                        zipout.putNextEntry(new ZipEntry(url));
                        zipout.write(nafnews.getBytes());

                    } catch (IOException e) {
                        e.printStackTrace();
                        System.err.print(" (PROBLEM ADDING TO ZIP) ");

                        return false;
                    }
                    return true;
                } else {
                    System.err.print(" (XML IS NOT VALID) ");
                    return false;
                }
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    static String getXML (String url, String news, Map header, String lang) throws ParserConfigurationException {
        Format format = Format.getPrettyFormat();
        format.setEncoding("UTF-8");
        XMLOutputter xml = new XMLOutputter(format);

        // root elements: <NAF xml:lang="en" version ="v3">
        Document doc = new Document();
        Element rootElement = new Element("NAF");
        Namespace nsp = Namespace.getNamespace("xml");
        rootElement.setAttribute("lang", lang, nsp.XML_NAMESPACE);
        rootElement.setAttribute("version", "v3");
        doc.addContent(rootElement);

        //header: <nafHeader>
        Element headerElement = new Element("nafHeader");
        Element filedescElement = new Element("fileDesc");
        try {
            if (header.containsKey("date"))
                filedescElement.setAttribute("creationtime", (String) header.get("date"));
            else {
                System.err.println("WARNING! File "+url+ " was skipped because the data is missed");
                return null;
            }

            if (header.containsKey("author"))
                filedescElement.setAttribute("author", (String) header.get("author"));
            if (header.containsKey("title"))
                filedescElement.setAttribute("title", (String) header.get("title"));
            if (header.containsKey("filename"))
                filedescElement.setAttribute("filename", (String) header.get("filename"));

            //extra attribute
            if (header.containsKey("section"))
                filedescElement.setAttribute("section", (String) header.get("section"));
            if (header.containsKey("magazine"))
                filedescElement.setAttribute("magazine", (String) header.get("magazine"));
            if (header.containsKey("publisher"))
                filedescElement.setAttribute("publisher", (String) header.get("publisher"));
            if (header.containsKey("location"))
                filedescElement.setAttribute("location", (String) header.get("location"));

            //if (header.containsKey("dateline"))
            //    filedescElement.setAttribute("dateline", (String) header.get("dateline"));
            headerElement.addContent(filedescElement) ;

            Element publicElement = new Element("public");
            if (header.containsKey("id"))
                publicElement.setAttribute("publicId", (String) header.get("id"));
            String uri = uriPrefix+
                    ((String) header.get("date")).replaceFirst("T.*","").replaceAll("-", "/")+"/"+url.replaceAll(".*/","").trim();
            publicElement.setAttribute("uri", URICleaner.cleanURI(uri).toString());
            headerElement.addContent(publicElement) ;

            rootElement.addContent(headerElement);

            //raw: <raw><![CDATA[Followers
            Element rawElement = new Element("raw");
            CDATA cdata=new CDATA(news);
            rawElement.setContent(cdata);
            rootElement.addContent(rawElement);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.print(" {"+ e.getMessage() + "}");
            return null;
        }
        return xml.outputString(rootElement);
    }


    public void zip2cgt (String cgtPath) throws IOException {
        try {
            File archiveFile = new File(cgtPath);
            if (archiveFile.getParentFile() != null && !archiveFile.getParentFile().exists())
                archiveFile.getParentFile().mkdirs();
            // out put file
            zipout = new ZipOutputStream(new FileOutputStream(cgtPath,false));

        } catch (Exception e) {
            e.printStackTrace();
        }

        ZipFile zf = new ZipFile(zipPath);
        try {
            for (Enumeration<? extends ZipEntry> e = zf.entries();
                 e.hasMoreElements();) {
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
            zipout.close();
        }
    }



    public static void main (String args[]) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage:\n  java main.java.org.fbk.hlt.newsreader.ZipXML2NAF <zip input FILE> <zip output FILE> <URI prefix> [MINSIZE] [MAXSIZE]");

            System.exit(0);
        }
        ZipXML2NAF zip = new ZipXML2NAF(args[0]);
        if (args.length > 2) {
            uriPrefix = args[2];
        }
        if (args.length > 3 && args[3].matches("[0-9]+")) {
            MINSIZE = Integer.valueOf(args[3]);
        }
        if (args.length > 4 && args[4].matches("[0-9]+")) {
            MAXSIZE = Integer.valueOf(args[4]);
        }
        zip.zip2cgt(args[1]);
    }

}


