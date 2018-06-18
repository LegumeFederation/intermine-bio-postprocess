package org.intermine.bio.postprocess;
/*
 * Copyright (C) 2002-2017 FlyMine, Legume Federation
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import org.intermine.model.bio.Author;
import org.intermine.model.bio.Publication;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.OrderDescending;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;
import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import org.ncgr.crossref.WorksQuery;
import org.ncgr.pubmed.PubMedSummary;

/**
 * Populate data and authors for publications from CrossRef and PubMed.
 *
 * @author Sam Hokin
 */
public class PopulatePublicationsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulatePublicationsProcess.class);

    // hold authors in a map so we don't store dupes; keyed by name
    Map<String,Author> authorMap = new HashMap<String,Author>();
    
    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulatePublicationsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     *
     * Main method
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void postProcess() throws ObjectStoreException {
        try {
            
            // delete existing Author objects, first loading them into a collection
            Query qAuthor = new Query();
            qAuthor.setDistinct(true);
            ConstraintSet csAuthor = new ConstraintSet(ConstraintOp.AND);
            // 0 Author
            QueryClass qcAuthor = new QueryClass(Author.class);
            qAuthor.addToSelect(qcAuthor);
            qAuthor.addFrom(qcAuthor);
            Set<Author> authorSet = new HashSet<Author>();
            Results authorResults = osw.getObjectStore().execute(qAuthor);
            Iterator<?> authorIter = authorResults.iterator();
            while (authorIter.hasNext()) {
                ResultsRow<?> rr = (ResultsRow<?>) authorIter.next();
                authorSet.add((Author)rr.get(0));
            }
            // delete them one by one (because bulk deletion is broken)
            // NOTE: this does not delete authorspublications records. That is a bug IMO.
            osw.beginTransaction();
            for (Author author : authorSet) {
                osw.delete(author);
            }
            osw.commitTransaction();
        
            // now query the publications
            Query qPub = new Query();
            qPub.setDistinct(true);

            // 0 Publication
            QueryClass qcPub = new QueryClass(Publication.class);
            qPub.addToSelect(qcPub);
            qPub.addFrom(qcPub);

            // execute the query
            Results pubResults = osw.getObjectStore().execute(qPub);
            Iterator<?> pubIter = pubResults.iterator();
            while (pubIter.hasNext()) {

                LOG.info("------------------------------------------------");

                ResultsRow<?> rrPub = (ResultsRow<?>) pubIter.next();
                Publication pub = (Publication) rrPub.get(0);

                // field values
                String title = stringOrNull(pub.getFieldValue("title"));
                String firstAuthor = stringOrNull(pub.getFieldValue("firstAuthor"));
                String lastAuthor = stringOrNull(pub.getFieldValue("lastAuthor"));
                String month = stringOrNull(pub.getFieldValue("month"));
                int year = intOrZero(pub.getFieldValue("year"));
                String journal = stringOrNull(pub.getFieldValue("journal"));
                String volume = stringOrNull(pub.getFieldValue("volume"));
                String issue = stringOrNull(pub.getFieldValue("issue"));
                String pages = stringOrNull(pub.getFieldValue("pages"));
                String abstractText = stringOrNull(pub.getFieldValue("abstractText"));
                int pubMedId = intOrZero(pub.getFieldValue("pubMedId"));
                String doi = stringOrNull(pub.getFieldValue("doi"));

                // try to snag the PMID and DOI if we have just a title
                if (doi==null && pubMedId==0 && title!=null) {
                    // query PubMed for PMID from title
                    PubMedSummary summary = new PubMedSummary(title);
                    if (summary.id==0) {
                        LOG.info("PMID not found from title:"+title);
                    } else {
                        pubMedId = summary.id;
                        LOG.info("PMID found from title; PMID="+pubMedId);
                        if (summary.doi!=null) {
                            doi = summary.doi;
                            LOG.info("DOI="+doi+" found from PMID="+pubMedId);
                        }
                    }
                }

                // get the DOI from PubMed if PMID exists and DOI does not
                if (doi==null && pubMedId!=0) {
                    // query PubMed for data from PMID, update everything in case CrossRef fails
                    PubMedSummary summary = new PubMedSummary(pubMedId);
                    if (summary.doi==null) {
                        LOG.info("DOI not found from PMID="+pubMedId);
                    } else {
                        doi = summary.doi;
                        LOG.info("DOI="+doi+" found from PMID="+pubMedId);
                    }
                }

                // query CrossRef entry from DOI or title/firstAuthor
                WorksQuery wq = null;
                boolean crossRefSuccess = false;
                JSONArray authors = null;
                String origTitle = title;
                if (doi!=null) {
                    wq = new WorksQuery(doi);
                    crossRefSuccess = (wq.getStatus()!=null && wq.getStatus().equals("ok"));
                    LOG.info("Queried DOI="+doi+" with result="+crossRefSuccess);
                } else if (firstAuthor!=null || origTitle!=null) {
                    wq = new WorksQuery(firstAuthor, origTitle);
                    crossRefSuccess = wq.isTitleMatched();
                    LOG.info("Queried firstAuthor="+firstAuthor+", origTitle="+origTitle+" with result="+crossRefSuccess);
                }

                if (crossRefSuccess) {

                    // update attributes from CrossRef if found
                    LOG.info("Found CrossRef match:"+wq.getDOI());
                    
                    title = wq.getTitle();
                    month = String.valueOf(wq.getIssueMonth());
                    year = wq.getIssueYear();
                    if (wq.getShortContainerTitle()!=null) {
                        journal = wq.getShortContainerTitle();
                    } else if (wq.getContainerTitle()!=null) {
                        journal = wq.getContainerTitle();
                    }
                    volume = wq.getVolume();
                    issue = wq.getIssue();
                    pages = wq.getPage();
                    doi = wq.getDOI();
                    authors = wq.getAuthors();
                    if (authors.size()>0) {
                        JSONObject firstAuthorObject = (JSONObject) authors.get(0);
                        firstAuthor = firstAuthorObject.get("family")+", "+firstAuthorObject.get("given");
                    }
                    if (authors.size()>1) {
                        JSONObject lastAuthorObject = (JSONObject) authors.get(authors.size()-1);
                        lastAuthor = lastAuthorObject.get("family")+", "+lastAuthorObject.get("given");
                    }

                    // update publication object
                    Publication tempPub = PostProcessUtil.cloneInterMineObject(pub);
                    if (title!=null) tempPub.setFieldValue("title", title);
                    if (firstAuthor!=null) tempPub.setFieldValue("firstAuthor", firstAuthor);
                    if (lastAuthor!=null) tempPub.setFieldValue("lastAuthor", lastAuthor);
                    if (month!=null && !month.equals("0")) tempPub.setFieldValue("month", month);
                    if (year>0) tempPub.setFieldValue("year", year);
                    if (journal!=null) tempPub.setFieldValue("journal", journal);
                    if (volume!=null) tempPub.setFieldValue("volume", volume);
                    if (issue!=null) tempPub.setFieldValue("issue", issue);
                    if (pages!=null) tempPub.setFieldValue("pages", pages);
                    if (pubMedId>0) tempPub.setFieldValue("pubMedId", String.valueOf(pubMedId));
                    if (doi!=null) tempPub.setFieldValue("doi", doi);
                    
                    if (authors!=null) {
                        
                        // update publication.authors from CrossRef since it provides given and family names; given often includes initials, e.g. "Douglas R"
                        LOG.info("Replacing publication.authors from CrossRef.");
                        
                        // place this pub's authors in a set to add to its authors collection; store the ones that are new
                        authorSet = new HashSet<Author>();
                        osw.beginTransaction();
                        for (Object authorObject : authors)  {
                            JSONObject authorJSON = (JSONObject) authorObject;
                            // IM Author attributes from CrossRef fields
                            String firstName = (String) authorJSON.get("given");
                            String lastName = (String) authorJSON.get("family");
                            // split out initials if present
                            // R. K. => R K
                            // R.K.  => R K
                            // Douglas R => Douglas R
                            // Douglas R. => Douglas R
                            String initials = null;
                            // deal with space
                            String[] parts = firstName.split(" ");
                            if (parts.length==2) {
                                if (parts[1].length()==1) {
                                    firstName = parts[0];
                                    initials = parts[1];
                                } else if (parts[1].length()==2 && parts[1].endsWith(".")) {
                                    firstName = parts[0];
                                    initials = parts[1].substring(0,1);
                                }
                            }
                            // pull initial out if it's an R.K. style first name (but not M.V.K.)
                            if (initials==null && firstName.length()==4 && firstName.charAt(1)=='.' && firstName.charAt(3)=='.') {
                                initials = String.valueOf(firstName.charAt(2));
                                firstName = String.valueOf(firstName.charAt(0));
                            }
                            // remove trailing period from a remaining R. style first name
                            if (firstName.length()==2 && firstName.charAt(1)=='.') {
                                firstName = String.valueOf(firstName.charAt(0));
                            }
                            // name is used as key, ignore initials since sometimes there sometimes not
                            String name = firstName+" "+lastName;
                            Author author;
                            if (authorMap.containsKey(name)) {
                                author = authorMap.get(name);
                            } else {
                                author = (Author) DynamicUtil.createObject(Collections.singleton(Author.class));
                                author.setFieldValue("firstName", firstName);
                                author.setFieldValue("lastName", lastName);
                                author.setFieldValue("name", name);
                                if (initials!=null) author.setFieldValue("initials", initials);
                                osw.store(author);
                                authorMap.put(name, author);
                                LOG.info("Added: "+firstName+"|"+initials+"|"+lastName+"="+name);
                            }
                            authorSet.add(author);
                        }
                        osw.commitTransaction();

                        // put these authors into the pub authors collection
                        tempPub.setFieldValue("authors", authorSet);
                    
                    }

                    // store this publication
                    osw.beginTransaction();
                    osw.store(tempPub);
                    osw.commitTransaction();

                } else if (pubMedId!=0) {

                    // get the publication from its PMID and summary
                    PubMedSummary summary = new PubMedSummary(pubMedId);
                    Publication tempPub = PostProcessUtil.cloneInterMineObject(pub);
                    populateFromSummary(tempPub, summary);
                    
                    // store this publication
                    osw.beginTransaction();
                    osw.store(tempPub);
                    osw.commitTransaction();

                }

            }

        } catch (IllegalAccessException ex) {
            throw new ObjectStoreException(ex);
        } catch (Exception ex) {
            LOG.error(ex);
        }

    }

    /**
     * Return the string value of a field, or null
     */
    String stringOrNull(Object fieldValue) {
        if (fieldValue==null) {
            return null;
        } else {
            return (String)fieldValue;
        }
    }

    /**
     * Return the int value of a field, or zero if null or not parseable into an int
     */
    int intOrZero(Object fieldValue) {
        if (fieldValue==null) {
            return 0;
        } else {
            try {
                int intValue = (int)(Integer)fieldValue;
                return intValue;
            } catch (Exception e1) {
                // probably a String, not Integer
                String stringValue = (String)fieldValue;
                try {
                    int intValue = Integer.parseInt(stringValue);
                    return intValue;
                } catch (Exception e2) {
                    return 0;
                }
            }
        }
    }

    /**
     * Parse the year from a string like "2017 Mar" or "2017 Feb 3"
     */
    static int getYear(String dateString) {
        String[] parts = dateString.split(" ");
        return Integer.parseInt(parts[0]);
    }

    /**
     * Parse the month from a string like "2017 Mar" or "2017 Feb 3"
     */
    static String getMonth(String dateString) {
        String[] parts = dateString.split(" ");
        if (parts.length>1) {
            return parts[1];
        } else {
            return null;
        }
    }

    /**
     * Populate Publication instance fields from the summary
     */
    void populateFromSummary(Publication publication, PubMedSummary summary) throws ObjectStoreException {
        LOG.info("Populating "+summary.id+" from PubMedSummary:");
        // mandatory fields
        publication.setFieldValue("title", summary.title);
	publication.setFieldValue("pubMedId", String.valueOf(summary.id));
        publication.setFieldValue("year", getYear(summary.pubDate));
        publication.setFieldValue("journal", summary.source);
        // optional fields
        if (getMonth(summary.pubDate)!=null) publication.setFieldValue("month", getMonth(summary.pubDate));
        if (summary.pages!=null && summary.pages.length()>0) publication.setFieldValue("pages", summary.pages);
        if (summary.issue!=null && summary.issue.length()>0) publication.setFieldValue("issue", summary.issue);
        if (summary.lastAuthor!=null && summary.lastAuthor.length()>0) publication.setFieldValue("lastAuthor", summary.lastAuthor);
        if (summary.doi!=null && summary.doi.length()>0) publication.setFieldValue("doi", summary.doi);
        if (summary.volume!=null && summary.volume.length()>0) publication.setFieldValue("volume", summary.volume);
        if (summary.authorList!=null && summary.authorList.size()>0) publication.setFieldValue("firstAuthor", summary.authorList.get(0));
        // the list of Author Items
        Set<Author> authorSet = new HashSet<Author>();
        osw.beginTransaction();
        for (String authorName : summary.authorList) {
            // PubMed has names like Close TJ or Jean M
            String[] parts = authorName.split(" ");
            String lastName = parts[0];
            String initials = parts[1];
            String firstInitial = String.valueOf(initials.charAt(0));
            String middleInitial = null;
            if (initials.length()>1) {
                middleInitial = String.valueOf(initials.charAt(1));
            }
            // name is used as key, ignore middle initials since sometimes there sometimes not
            String name = firstInitial+" "+lastName;
            Author author;
            if (authorMap.containsKey(name)) {
                author = authorMap.get(name);
            } else {
                author = (Author) DynamicUtil.createObject(Collections.singleton(Author.class));
                author.setFieldValue("firstName", firstInitial);
                author.setFieldValue("lastName", lastName);
                if (middleInitial!=null) author.setFieldValue("initials", middleInitial);
                author.setFieldValue("name", name);
                osw.store(author);
                authorMap.put(name, author);
                LOG.info("Added: "+firstInitial+"|"+middleInitial+"|"+lastName+"="+name);
            }
            authorSet.add(author);
        }
        osw.commitTransaction();

        // put these authors into the pub authors collection
        publication.setFieldValue("authors", authorSet);

    }

}
