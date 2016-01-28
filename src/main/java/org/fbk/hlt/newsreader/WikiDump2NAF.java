package main.java.org.fbk.hlt.newsreader;

import eu.fbk.nwrtools.URICleaner;
import fbk.hlt.utility.archive.CatalogOfGzippedTexts;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import org.jdom.CDATA;
import org.jdom.Document;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Demo application which reads a compressed or uncompressed Wikipedia XML dump
 * file (depending on the given file extension <i>.gz</i>, <i>.bz2</i> or
 * <i>.xml</i>) and prints the title and wiki text.
 *
 */
public class WikiDump2NAF {
    /**
     * Print title an content of all the wiki pages in the dump.
     *
     */
    final static Pattern regex = Pattern.compile("[A-Z][\\p{L}\\w\\p{Blank},\\\"\\';\\[\\]\\(\\)-]+[\\.!]",
            Pattern.CANON_EQ);
    static String language = "";
    static  int counter = 0;
    private static final int quotaEntries = 100000; //6200;
    static int nafArchiveCounter = 1;
    static final int LIMIT_CHARS = 200;
    static String outputDir = "/tmp/";
    static CatalogOfGzippedTexts cgtArchive = null;
    static CatalogOfGzippedTexts nafArchive = null;
    static Pattern datePattern = Pattern.compile("[^\\d]*(\\d+)[^\\d]*,*\\s*(\\d\\d\\d\\d)");
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    static class DemoArticleFilter implements IArticleFilter {
        // Convert to plain text
        WikiModel wikiModel = new WikiModel("${image}", "${title}");

        @Override
        public void process(WikiArticle page, Siteinfo siteinfo) throws SAXException {

            if (page != null && page.getText() != null) {
                if(!page.isTemplate() && page.isMain()
                        //&& page.getId().equals("4220")
                        //&& page.getId().equals("4812")
                        //&& page.getId().equals("981")
                        //&& page.getId().equals("1007")
                        //&& page.getId().equals("6690")

                        ///TEST SET (English)
                        //&& (page.getId().equals("279494") || page.getId().equals("23973") || page.getId().equals("199629")
                        //|| page.getId().equals("238039") || page.getId().equals("287096") || page.getId().equals("100154"))
                        ///TEST SET (Italian)
                        //&& (page.getId().equals("1119") || page.getId().equals("1576"))

                        //279494 http://en.wikinews.org/wiki/Apple_executive_Steve_Jobs_resigns
                        //199629 http://en.wikinews.org/wiki/Apple_executive_leaves_company_after_iPhone_4_antenna_issues
                        //23973 http://en.wikinews.org/wiki/Apple_introduces_new_iPod_with_video_playback_capabilities
                        //238039 http://en.wikinews.org/wiki/Apple_Inc._CEO_Steve_Jobs_on_medical_leave
                        //287096 http://en.wikinews.org/wiki/Apple_Inc._co-founder_Steve_Jobs_dies_aged_56
                        //&& (page.getId().equals("35993")
                        //|| page.getId().equals("17300"))
                        && !page.isCategory() && !page.getText().startsWith("#REDIRECT ")) {
                    LinkedHashMap<String,String> mapValues = new LinkedHashMap<String, String>();


                    /*System.out.println("----------------------------------------");
                    System.out.println(page.getId());
                    System.out.println(page.getRevisionId());
                    System.out.println(page.getTimeStamp());
                    System.out.println(page.getTitle());
                    System.out.println("----------------------------------------");
                      */
                    String id =  page.getId();
                    mapValues.put("id",id);
                    String title = page.getTitle().trim();
                    mapValues.put("title", title);



                    String myurl = "http://"+language+".wikinews.org/wiki/"+title.replaceAll(" ","_");
                    //mapValues.put("url",myurl);
                    String wikiText = page.toString();
                    //System.err.println(page.toString());
                    //remove images
                    wikiText = wikiText.replaceAll("\\[\\[[^\\]]+\\|right\\]\\]"," ");
                    wikiText = wikiText.replaceAll("\\[\\[[^\\]]+\\|left\\]\\]"," ");

                    wikiText = wikiText.replaceAll("\n","##BR##");
                    wikiText = wikiText.replaceFirst("&lt;gallery&gt;.*", "");
                    wikiText = wikiText.replaceFirst("\\{\\{haveyoursay.*","");
                    wikiText = wikiText.replaceAll("==+[^=]+==+.*", " ");

                    //elimino le tabelle
                    wikiText = wikiText.replaceAll("\\{\\|.+\\|\\}","");
                    //elimino bold e corsivo
                    wikiText = wikiText.replaceAll("'''*","");
                    //elimino parti non della pagina  (menu, ...)
                    wikiText = wikiText.replaceFirst("__NOEDITSECTION__.*","");

                    wikiText = wikiText.replaceAll("(?m)<ref>.+</ref>", " ").
                            replaceAll("(?m)<ref name=\"[A-Za-z0-9\\s-]+\">.+</ref>", " ").
                            replaceAll("<ref>", " <ref>");


                    String[] items = wikiText.replaceAll("\\{\\{Cquote\\|","##BR##{{w|").split("\\{\\{w\\|");
                    wikiText = "";
                    int l = 0;
                    for (String ic : items)  {
                        if (l==0) {
                            wikiText = ic;
                            l=1;
                        } else {
                            String naming = ic.replaceFirst("}}.+","");
                            wikiText += naming.replaceAll(".+\\|","")+ic.replaceFirst("[^}]+}}","");
                        }
                    }

                    // Remove text inside {{ }}
                    String content = wikiModel.render(new PlainTextConverter(), wikiText);
                    if (content.contains("{{")) {
                        System.err.println(">>>> "+ myurl +"\t"+
                                content.replaceAll("\\}\\}[^\\{]*\\{\\{",",").replaceFirst(".*\\{\\{","").replaceFirst("\\}\\}.*",""));
                        content = content.replaceAll("\\{\\{[^\\}]+\\}\\}", " ");
                    }
                    content = content.replaceAll("File:.+", " ").replaceAll("Categor.+:.+", " ");
                    //System.err.println(content.replaceAll("##BR##","\n"));

                    //remove references
                    content = content.replaceAll("\\s*\\[[0-9]+\\]\\s*"," ").trim();
                    //remove title
                    if (content.startsWith(title))
                        content = content.substring(title.length()).trim();
                    content = content.replaceFirst("^(\\s*##BR##\\s*)+","").replaceFirst("(\\s*##BR##\\s*)+$","##BR##");
                    content = content.replaceAll("\\]\\]","").trim();
                    //get categories
                    items = page.toString().split("\\[\\[\\s*");
                    String categories = "";
                    for (String item : items)  {
                        item = item.replaceAll("[\r|\n]","");
                        if (item.toLowerCase().matches("^categor.+:.*$")) {
                            if (item.matches(".+\\d, ?\\d\\d\\d\\d\\]\\].*")) {
                                mapValues.put("date", item.replaceAll("\\]\\].*","").replaceAll(".+:", "").replaceAll("\\|.*","").trim());
                            }
                            categories += ", " + item.replaceAll("\\]\\].*","").replaceAll(".+:", "").replaceAll("\\|.*","");
                        }

                        if (item.matches("^[a-z][a-z]:.+")) {
                            content = content.replaceFirst(item.replaceAll("\\]\\].*",""),"");
                            item = item.replaceFirst("\\]\\].*","");
                            String lang = item.replaceFirst(":.+","");
                            String link = "http://"+lang+".wikinews.org/wiki/" +item.replaceFirst("..:","").replaceAll("\\|.*","").trim().replaceAll(" ","_");
                            try {
                                //System.err.println(lang + " " + new String(link.getBytes("UTF8")));
                                mapValues.put("languages/link:lang&" + lang, new String(link.getBytes("UTF8")));
                            } catch (UnsupportedEncodingException e) {
                                System.err.println("ERROR on " +myurl + " (link: "+link+")");
                            }
                        }

                    }
                    if (categories.length() > 0) {
                        mapValues.put("categories", categories.replaceFirst(", ","").replaceAll("##BR##","").replaceAll("[\\]|\\}][\\]|\\}]",""));
                    }


                    //trovo i link alle sources
                    getSources(page.toString(), mapValues);

                    if (content.replaceAll("##BR##","").length() > LIMIT_CHARS && !content.contains("|") && mapValues.containsKey("date")) {
                        //System.out.println("## " + myurl+"\n"+content.replaceAll("##BR##","\n").trim());
                        if (mapValues.containsKey("date")) {
                            String[] months = {"january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"};
                            if (language.equals("it"))
                                months = new String[]{"gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno", "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"};
                            else if (language.equals("es"))
                                months = new String[]{"enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"};
                            else if (language.equals("nl"))
                                months = new String[]{"januari","februari","maart","april","mei","juni","juli","augustus","september","oktober","november","december"};

                            String date = mapValues.get("date");
                            mapValues.remove("date");

                            if (date.matches("\\d\\d\\d\\d-\\d\\d-\\d\\d")) {
                                //System.err.println("\n@@1\t" + mapValues.get("id") +"\t" +date + "\t" + myurl);
                                mapValues.put("date", date);
                            } else {
                                //System.err.println("DATE " + date);
                                int i=1;
                                for (String month : months) {
                                    if (date.toLowerCase().contains(month)) {// && date.matches(month +"\\s+\\d+,\\s*\\d\\d\\d\\d")) {
                                        mapValues.put("textdate",date);
                                        Matcher matchDate = datePattern.matcher(date);
                                        if (matchDate.find()) {
                                            //System.err.println("====> " + matchDate.group(2)+"/"+i+"/"+matchDate.group(1));
                                            date = matchDate.group(2) + "-";
                                            if (i< 10) {
                                                date += "0";
                                            }
                                            date += i+"-";
                                            if (matchDate.group(1).length() == 1) {
                                                date += "0";
                                            }
                                            date += matchDate.group(1);

                                            //System.err.println("\n@@2\t"+ mapValues.get("id") +"\t" + date + "\t" + myurl);
                                            mapValues.put("date", date);
                                            break;
                                        }
                                    }
                                    i++;
                                }
                                if (!mapValues.containsKey("date")) {
                                    //mapValues.put("nodate",date);
                                    System.err.println("\nNODATE\t"+ id +"\t" + date + "\t" + myurl);
                                    //System.exit(0);
                                }
                            }

                        } else {
                            System.err.println(">> NO DATE for " + myurl);
                            //Thread.sleep(3000);
                        }

                        //create NAF archive
                        try {
                            String date = "";
                            if (mapValues.containsKey("date"))
                                date =  mapValues.get("date");
                            String xml = getXML(myurl, content, id, date, title);
                            if (xml.length() > 0) {
                                counter++;
                                if (counter > quotaEntries) {
                                    nafArchive.close();
                                    counter = 0;
                                    nafArchiveCounter++;
                                    try {
                                        nafArchive = new CatalogOfGzippedTexts(outputDir+language+"-wikinews_NAF-"+nafArchiveCounter+".cgt", "rw");
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                nafArchive.add(language+"wikinews-"+id+".xml",xml.getBytes("UTF8"),null);
                            }

                            if (cgtArchive.contains(myurl))
                                System.err.println("ALREADY FOUND: " + myurl);
                            else {
                                cgtArchive.add(myurl, content.getBytes("UTF8"), mapValues);
                            }
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            System.exit(0);
                        } catch (ParserConfigurationException e) {
                            e.printStackTrace();
                            System.exit(0);
                        }

                        //System.exit(0);
                    } else {
                        System.err.println("SKIPPED: " +page.getId() + " (" +page.getText().length() +" len, isProject:"+page.isProject() + " isMain:" +page.isMain() + ") " +page.getTitle());

                        //System.err.println(page.getText()+"\n-------\n"+content.replaceAll("##BR##","")+"\n=======\n");
                    }
                }

                /*for (String key : mapValues.keySet()) {
                    System.err.println(key +": "+mapValues.get(key));
                } */
                //System.err.flush();
                //System.out.println(content);
            }
        }
    }

    static String getXML (String url, String news, String id, String date, String title) throws ParserConfigurationException, UnsupportedEncodingException {
        Format format = Format.getPrettyFormat();
        format.setEncoding("UTF-8");
        XMLOutputter xml = new XMLOutputter(format);

        // root elements: <NAF xml:lang="en" version ="v3">
        Document doc = new Document();
        org.jdom.Element rootElement = new org.jdom.Element("NAF");
        Namespace nsp = Namespace.getNamespace("xml");
        rootElement.setAttribute("lang", language, nsp.XML_NAMESPACE);
        rootElement.setAttribute("version", "v3");
        doc.addContent(rootElement);

        //header: <nafHeader>
        org.jdom.Element headerElement = new org.jdom.Element("nafHeader");
        org.jdom.Element filedescElement = new org.jdom.Element("fileDesc");
        if (date.length() > 0)
            filedescElement.setAttribute("creationtime", getISODate(date));
        if (title.length() > 0) {
            //filedescElement.setAttribute("title", title);
            //filedescElement.setAttribute("title", new String(title.getBytes(),"UTF8"));  //Fu�bal
            //filedescElement.setAttribute("title", new String(title.getBytes("UTF8")));     //Fu√üball
            //filedescElement.setAttribute("title", new String(title.getBytes(),"Latin1"));    //Fu§ball
            //filedescElement.setAttribute("title", new String(title.getBytes("Latin1"),"UTF8"));    //Fu�bal
            filedescElement.setAttribute("title", new String(title.getBytes()));    //l
        }
        //if (header.containsKey("dateline"))
        //    filedescElement.setAttribute("dateline", (String) header.get("dateline"));
        headerElement.addContent(filedescElement) ;

        org.jdom.Element publicElement = new org.jdom.Element("public");
        if (id.length() > 0)
            publicElement.setAttribute("publicId", id);
        publicElement.setAttribute("uri", URICleaner.cleanURI(url).toString());
        headerElement.addContent(publicElement) ;

        rootElement.addContent(headerElement);

        //raw: <raw><![CDATA[Followers
        org.jdom.Element rawElement = new org.jdom.Element("raw");
        CDATA cdata=new CDATA(news.replaceAll("##BR##","\n"));
        rawElement.setContent(cdata);
        rootElement.addContent(rawElement);

        //System.out.println(xml.outputString(rootElement));
        return xml.outputString(rootElement);
    }

    static void getSources(String content, LinkedHashMap<String, String> lhash) {
        String[] lines = content.split("\n");
        boolean external = false;
        int link=1;
        for (String line : lines) {
            String datelable = "date";
            if (language.equals("it"))
                datelable="data";
            else if (language.equals("es"))
                datelable="fecha";
            else if (language.equals("nl"))
                datelable="datum";

            if (!lhash.containsKey("date") && line.toLowerCase().matches(".*\\{\\{.*"+datelable+"\\s*[\\||=].+")) {
                //System.err.println("DLINE " +line);
                String date = line.toLowerCase().replaceFirst(".+\\s*"+datelable+"\\s*[\\||=]","").replaceFirst("[\\||\\}].+","");
                date = date.replaceFirst("^.+=","");
                if (date.trim().length() > 0) {
                    lhash.put("date",date);
                }
            }
            if (line.matches(".*\\{\\{source\\|url=.*")) {
                //System.err.println("DLINE2 " +line);

                if (!lhash.containsKey("date")) {
                    String date = line.toLowerCase().replaceFirst(".+\\|\\s*date=","").replaceFirst("\\|.+","");
                    if (date.trim().length() > 0) {
                        lhash.put("date",date);
                    }
                }
                line = line.replaceFirst(".*\\{\\{source\\|url=\\s*","").replaceAll("\\|.*","").trim();
                //System.err.println("SOURCE: " +line);
                lhash.put("sources/link:num&"+link, line);
                link++;
            }


            if (line.matches(".*== External links ==.*")) {
                external = true;
            }
            if (external && line.contains("[http")) {
                line = line.replaceFirst(".+\\[","").replaceFirst("\\s.*$","").replaceAll("\\|.*","").trim();
                lhash.put("extlinks/link:num&"+link, line);
                link++;
                //System.err.println("EXTERNAL: " +line);
            }
        }

    }


    public static String getISODate (String date) {
        //TimeZone tz = TimeZone.getTimeZone("CET");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        //df.setTimeZone(tz);
        if (date != null) {
            try {
                return df.format(sdf.parse(date));
            } catch (ParseException e) {
                System.err.println("WARNING! " +e.getMessage());
            }
        }
        return df.format(new Date());
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: WikiDump2NAF <LANG> <XML-FILE> <ARCHIVE OUTPUT-DIR>");
            System.exit(-1);
        }
        language = args[0];
        // String bz2Filename =
        // "c:\\temp\\dewikiversity-20100401-pages-articles.xml.bz2";
        String bz2Filename = args[1];
        outputDir = args[2];
        if (!outputDir.endsWith(File.separator))
            outputDir +=File.separator;
        try {
            nafArchive = new CatalogOfGzippedTexts(outputDir+language+"-wikinews_NAF-"+nafArchiveCounter+".cgt", "rw");
            cgtArchive = new CatalogOfGzippedTexts(outputDir+"wikinews-"+language+".cgt", "rw");

            IArticleFilter handler = new DemoArticleFilter();
            WikiXMLParser wxp = new WikiXMLParser(bz2Filename, handler);
            wxp.parse();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}