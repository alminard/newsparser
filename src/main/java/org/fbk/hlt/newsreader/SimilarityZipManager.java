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
import fbk.hlt.utility.archive.CatalogOfGzippedTexts;
import org.jdom.CDATA;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.*;
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Author: Christian Girardi (cgirardi@fbk.eu)
 * Date: 27-mag-2013
 *
 * Dato un file zip con le news di LN crea il file cgt con le news che matchano maggiormente con i vettori
 *
  <file id="1">
   <head>
     <url>2013/1/10/57G4-5521-F12F-F038.xml</url>
     <company_sim>0.0542897729980556</company_sim>
     <date>20130110</date>
     <dtime>Wed Nov 06 04:56:23 CET 2013</dtime>
     <encoding>UTF8</encoding>
     <keys_sim>0.013692065594242851</keys_sim>
     <pagesize>1554</pagesize>
     <title>AUDI PIPS MERC AS NO. 2 LUXURY CAR BRAND IN INDIA</title>
     <wordnum>274</wordnum>
     <charnum>1546</charnum>
   </head>
   <content>AUDI PIPS MERC AS NO. 2 LUXURY CAR BRAND IN INDIA New Delhi, Jan. 10 -- It's official now. German luxury car maker Audi has overtaken Mercedes to grab the number 2 spot in the domestic luxury car market. Audi sold 9,003 cars in 2012 a growth of more than 63% over 2011 and benefited from Mercedes' lacklustre show during the year. The latter sold 7,138 units during the year, a decline of 4% over the previous year. Audi is just 372 units behind BMW, which has held on to its number 1 position for the fourth consecutive year despite registering stagnant sales. "We set a target of 8,000 cars for the year 2012 but managed to achieve this target by November itself and revised it further to 8,600 cars," said Michael Perschke, head, Audi India. "We have managed to exceed this target as well," said Perschke. Mercedes has now slipped in the rankings from number 1 to 3rd spot within a span of 5 years but is banking on compact cars A and B class to haul it back into the game. "Our sales performance has been overall in line with expectations," said Eberhard Kern, managing director and CEO, Mercedes Benz India. "At the same time we have managed to retain our profitability and premium in 2012. 2013 would be an exciting year for is." Mercedes is targeting a return to the podium globally by the turn of this decade and in India ahead of that. Published by HT Syndication with permission from Hindustan Times. For any query with respect to this article or any other content requirement, please contact Editor at htsyndication@hindustantimes.com</content>
 </file>

 */

public class SimilarityZipManager {
    private DefaultHandler handler;
    private SAXParser saxParser;
    private String newsUrl = "";
    private File cgtFile;
    private String zipPath;
    private CatalogOfGzippedTexts cgtarchive;
    private static DocumentVector masterdoc_companies = new DocumentVector();
    private static DocumentVector masterdoc_keys = new DocumentVector();
    private int quotaDoc = -1;
    private int archiveNumber = 0;
    private String BASEURI = "http://www.newsreader-project.eu/";
    private String[] csvfields = {"uri","id","date","section","magazine","author","location","title","publisher"};

    /*nafID
    /nitf/head/docdata/doc-id#id-string
    /nitf/head/docdata/date.issue#norm
    /nitf/head/pubdata#position.section
    /nitf/head/pubdata#name
    /nitf/body/body.head/byline    author
    /nitf/body/body.head/dateline  location
    /nitf/body/body.head/headline/hl1 title
    /nitf/head/docdata/doc.copyright#holder
      */

    private int getCGTSize () {
        if (cgtarchive != null)
            return cgtarchive.size();
        return 0;
    }

    public SimilarityZipManager(String zipPath, String cgtPath, int quota) {
        this.cgtFile = new File(cgtPath);
        this.quotaDoc = quota;
        this.cgtarchive = createArchive();

        this.zipPath = zipPath;
        //String car_companies = "Audi,Bentley,Bugatti,Lamborghini,Porsche,Seat,Skoda,Volvo,Aston Martin,BMW,Mini,Rolls Royce,Fiat,Alfa Romeo,Chrysler,Dodge,Ferrari,Jeep,Lancia,Maserati,Ram,Ford,Lincoln,Mazda,GM,General Motors,Buick,Cadillac,Chevrolet,GMC,Daewoo,Opel,Vauxhall,Holden,Honda,Acura,Hyundai,Kia,Tata Motors,Jaguar,Land Rover,LandRover, Mazda,Mitsubishi,Daimler AG,Daimler,Mercedes,Mercedes-Benz,Smart,Nissan,Infiniti,Renault,Saab,National Electric Vehicle Sweden,NEVS,Subaru,Fuji Heavy Industries,Toyota,Tesla,Lexus,Scion,Daihatsu,Hino Motors,Fuji Industries,Isuzu,Peugeot,Citroën,Citroen";
        String car_companies = "Alfa Romeo,Aston Martin,Audi,BMW,Bentley,Bugatti,Buick,Cadillac,Chevrolet,Chrysler,Daewoo,Daihatsu,Daimler,De Lorean,De Tomaso,Dodge,Ferrari,Fiat,Ford,GMC,Honda,Hummer,Hyundai,Isuzu,Iveco,Jeep,Kia,Koenigsegg,Lada,Lamborghini,Lancia,Land Rover,Landrover,Lexus,Lincoln,Lotus,Maserati,Maybach,Mazda,McLaren,Mercedes,Mercedes-Benz,Mercury,Mitsubishi,Nissan,Oldsmobile,Opel,PAZ,Peugeot,Pontiac,Porsche,Proton,Renault,Rolls-Royce,Rolls Royce,SEAT,Saab,Scania,SsangYong,Steyr,Subaru,Suzuki,TVR,Texmaco,Toyota,Vauxhall,Volkswagen,Volvo,Yamaha,Yugo,Zephyr,Skoda,Citroen,Citroën,DAF,Jaguar,MG,Mercedes,Morgan,Morris,Plymouth,Puch,Rover,Saturn,Smart";
        for(String w:car_companies.split("[^a-zA-Zë\\-]+")) {
            System.err.println("CAR "+w);
            masterdoc_companies.incCount(w);
        }
        String keywords = "ownership,ownerships,joint venture,merge,merged,mergeder,mergedest,merger,mergers,merging,merginger,mergingest,mergings,acquisi,acquisition,acquisitions,acquisitiver,acquisitivest,acquisitive,CEO,manager,managerial,managerialer,managerialest,managers,managership,managerships,managing,director,directors,directorship,directorships,takeover,subsidiarier,subsidiaries,subsidiariest,subsidiarities,subsidiarity,subsidiary,headquarter,child entity,consolidate,consolidated,consolidateder,consolidatedest,consolidates,consolidating,consolidation,consolidations,consolidative,consolidativer,consolidativest,acquire,acquires,acquirer,acquiring,acquirable,acquirement,acquirements";
        for(String w:keywords.split("[^a-zA-Z]+")) {
            System.err.println("KEY "+w);
            masterdoc_keys.incCount(w);
        }


        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(false);


        try {
            //factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            saxParser = factory.newSAXParser();

            handler = new DefaultHandler() {

                String currentElement = "";
                StringBuilder body = new StringBuilder();
                StringBuilder currentContent = new StringBuilder();
                LinkedHashMap<String,String> header = new LinkedHashMap<String,String>();
                private int counter = 0;

                public void startElement(String uri, String localName,String qName,
                                         Attributes attributes) throws SAXException {
                    if (qName.equalsIgnoreCase("hl1")) {
                        currentContent.setLength(0);
                    } else if (qName.equalsIgnoreCase("block")) {
                        currentElement="block";
                    } else if ((currentElement.equalsIgnoreCase("block") && qName.equalsIgnoreCase("p")) || currentElement.equalsIgnoreCase("hl1") ) {
                        currentElement="relevantElement";
                    } else if (qName.equalsIgnoreCase("byline")) {
                        currentElement="byline";
                        currentContent.setLength(0);
                    } else if (qName.equalsIgnoreCase("pubdata")) {
                        header.put("section", attributes.getValue("position.section"));
                        header.put("magazine", attributes.getValue("name"));
                    } else if (qName.equalsIgnoreCase("doc.copyright")) {
                        header.put("publisher", attributes.getValue("holder"));
                    } else if (qName.equalsIgnoreCase("date.issue")) {
                        String date = attributes.getValue("norm");
                        if (date != null) {
                            date = date.substring(0,8);
                            header.put("date", date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8));
                        }
                    } else if (qName.equalsIgnoreCase("doc-id")) {
                        String id = attributes.getValue("id-string");
                        if (id == null)
                            id = java.util.UUID.randomUUID().toString();
                        header.put("id", id);
                    } else if (qName.equalsIgnoreCase("dateline")) {
                        currentElement="location";
                        currentContent.setLength(0);
                    }

                }

                public void endElement(String uri, String localName, String qName) throws SAXException {

                    if (qName.equalsIgnoreCase("hl1")) {
                        header.put("title", currentContent.toString());
                        body.append(currentContent.toString()).append("\n\n");
                        currentContent.setLength(0);
                    } else if (currentElement.equalsIgnoreCase("relevantElement") &&
                            qName.equalsIgnoreCase("p")) {
                        body.append("\n");
                        currentElement = "block";
                    } else if (qName.equalsIgnoreCase("br")) {
                        body.append("\n");
                    } else if (qName.equalsIgnoreCase("block")) {
                        body.append("\n");
                        currentElement="";
                    } else if (qName.equalsIgnoreCase("byline")) {
                        header.put("author", currentContent.toString());
                    } else if (qName.equalsIgnoreCase("dateline")) {
                        header.put("location", currentContent.toString());
                    } else if (qName.equalsIgnoreCase("body")) {
                        if (body.length() > 0) {
                            newsUrl = BASEURI + newsUrl;
                            header.put("uri",newsUrl);
                            //System.err.print("> " + newsUrl + "\n" + body);
                            System.err.print("> " + newsUrl);
                            header.put("encoding", "UTF8");

                            // store the metadata as Map and the body into the cgt archive
                            if (storeNews(newsUrl, body.toString(), header)) {
                                counter++;
                                System.err.println(" ...STORED ["+ counter +"]");

                                System.out.print("# ");
                                for (String field : csvfields) {
                                    if (header.get(field) != null)
                                        System.out.print(header.get(field).replaceAll("[\t|\r|\n]+", " "));

                                    System.out.print("\t");
                                }
                                System.out.println(body.length());
                            } else {
                                System.err.println(" ...SKIPPED! ["+body.toString().length() + " chars]");
                            }
                        }
                        body.setLength(0);
                        header.clear();
                        //System.exit(0);
                    }
                    //currentElement="";
                }

                public void characters(char ch[], int start, int length) throws SAXException {
                    if (currentElement.equalsIgnoreCase("relevantElement")) {
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

    boolean storeNews (String url, String news, Map header) {
        DocumentVector v1 = new DocumentVector();
        for(String w:news.split("[^a-zA-Zë\\-]+")) {
            v1.incCount(w);
        }

        header.put("company_sim", v1.getCosineSimilarityWith(masterdoc_companies));
        header.put("keys_sim", v1.getCosineSimilarityWith(masterdoc_keys));
        System.err.print(" ["+header.get("company_sim") + " " +header.get("keys_sim") +"] " + news);
        if ((Double)header.get("company_sim") > 0.00 && (Double)header.get("keys_sim") > 0) {
            String nafnews = null;
            try {
                nafnews = getXML(url, news, header);
                if (nafnews != null) {
                    if (quotaDoc > 0 && cgtarchive.size() >= quotaDoc) {
                        cgtarchive.close();
                        System.err.println("The archive " + cgtarchive.getFilepath() + " contains " +quotaDoc+ " docs. [quota reached]");
                        this.cgtarchive = createArchive();
                        //System.exit(0);
                    }
                    return cgtarchive.add(url, nafnews.getBytes(), header);
                }
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }

        }

        return false;

    }

    public void parser() throws IOException {
        System.err.println(">> " +this.zipPath);
        ZipFile zf = new ZipFile(this.zipPath);
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
        }
    }

    //create a new archive name increments with a progress numbering
    private CatalogOfGzippedTexts createArchive() {
        try {
            if (cgtFile.getParent() != null && !cgtFile.getParentFile().exists())
                cgtFile.getParentFile().mkdirs();
            CatalogOfGzippedTexts cgt = new CatalogOfGzippedTexts(cgtFile.getCanonicalPath(), "rw");
            if (quotaDoc < 0 || cgt.size() < quotaDoc) {
                return cgt;
            } else {
                archiveNumber++;
                File cgtpath = new File(cgtFile.getParentFile().getCanonicalPath()+File.separator+
                        cgtFile.getName().replaceFirst(".cgt$","-"+archiveNumber+".cgt"));
                while (cgtpath.exists()) {
                    archiveNumber++;
                    cgtpath = new File(cgtFile.getParentFile().getCanonicalPath()+File.separator+
                            cgtFile.getName().replaceFirst(".cgt$","-"+archiveNumber+".cgt"));

                }
                boolean success = cgtFile.renameTo(cgtpath);
                if (success) {
                    File setpath = new File(cgtFile.getCanonicalPath().replaceFirst(".cgt$",".set"));
                    if (setpath.delete()) {
                        return new CatalogOfGzippedTexts(cgtFile.getCanonicalPath(), "rw");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    static String getXML (String url, String news, Map header) throws ParserConfigurationException {
        Format format = Format.getPrettyFormat();
        format.setEncoding("UTF-8");
        XMLOutputter xml = new XMLOutputter(format);

        // root elements: <NAF xml:lang="en" version ="v3">
        Document doc = new Document();
        Element rootElement = new Element("NAF");
        Namespace nsp = Namespace.getNamespace("xml");
        rootElement.setAttribute("lang", "en", nsp.XML_NAMESPACE);
        rootElement.setAttribute("version", "v3");
        doc.addContent(rootElement);

        //header: <nafHeader>
        Element headerElement = new Element("nafHeader");
        Element filedescElement = new Element("fileDesc");
        if (header.containsKey("date"))
            filedescElement.setAttribute("creationtime", (String) header.get("date"));
        //if (header.containsKey("dateline"))
        //    filedescElement.setAttribute("dateline", (String) header.get("dateline"));
        headerElement.addContent(filedescElement) ;

        Element publicElement = new Element("public");
        if (header.containsKey("id"))
            publicElement.setAttribute("publicId", (String) header.get("id"));
        publicElement.setAttribute("uri", URICleaner.cleanURI(url).toString());
        headerElement.addContent(publicElement) ;

        rootElement.addContent(headerElement);

        //raw: <raw><![CDATA[Followers
        Element rawElement = new Element("raw");
        CDATA cdata=new CDATA(news);
        rawElement.setContent(cdata);
        rootElement.addContent(rawElement);

        //System.out.println(xml.outputString(rootElement));
        return xml.outputString(rootElement);
    }

    //
    public static void main (String args[]) throws Exception {
        int quota = -1;
        if (args.length > 2) {
            quota = Integer.valueOf(args[2]);
        }
        SimilarityZipManager zip = new SimilarityZipManager(args[0],args[1],quota);

        zip.parser();

        System.err.println("The archive " + args[1] +" contains " +zip.getCGTSize() + " documents.");

    }

}


