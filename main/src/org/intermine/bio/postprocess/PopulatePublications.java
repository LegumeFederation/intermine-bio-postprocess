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
public class PopulatePublications {

    private static final Logger LOG = Logger.getLogger(PopulatePublications.class);
    protected ObjectStoreWriter osw;

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulatePublications(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Run the analysis.
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void populatePublications() throws ObjectStoreException, IllegalAccessException {
        
        // hold authors built from CrossRef in a map so we don't store dupes; keyed by name
        Map<String,Author> authorMap = new HashMap<String,Author>();
        
        Query qPub = new Query();
        qPub.setDistinct(true);
        ConstraintSet csPub = new ConstraintSet(ConstraintOp.AND);

        // 0 Publication
        QueryClass qcPub = new QueryClass(Publication.class);
        qPub.addToSelect(qcPub);
        qPub.addFrom(qcPub);

        // execute the query
        Results pubResults = osw.getObjectStore().execute(qPub);
        Iterator<?> pubIter = pubResults.iterator();
        while (pubIter.hasNext()) {
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
            LOG.info("------------------------------------------------");
            LOG.info(firstAuthor);
            LOG.info(title);
            try {

                // query CrossRef entry, update attributes if found
                WorksQuery wq = new WorksQuery();
                boolean crossRefSuccess = false;
                JSONArray authors = null;
                String origTitle = title;
                if (doi!=null) {
                    wq.queryDOI(doi);
                    crossRefSuccess = (wq.getStatus()!=null && wq.getStatus().equals("ok"));
                } else if (firstAuthor!=null && title!=null) {
                    wq.queryAuthorTitle(firstAuthor, origTitle);
                    crossRefSuccess = wq.isTitleMatched();
                }
                if (crossRefSuccess) {
                    LOG.info("Found CrossRef match:"+wq.getDOI());
                    // update everything from CrossRef
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
                }

                if (pubMedId==0) {
                    // query PubMed for PMID
                    PubMedSummary summary = new PubMedSummary(title);
                    if (summary.id==0) {
                        LOG.info("PMID not found.");
                    } else {
                        pubMedId = summary.id;
                        LOG.info("PMID="+summary.id);
                    }
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

                    // update publication.authors from CrossRef since it provides given and family names
                    LOG.info("Replacing publication.authors from CrossRef.");

                    // delete existing Author objects, first loading them into a collection
                    Query qAuthor = new Query();
                    qAuthor.setDistinct(true);
                    ConstraintSet csAuthor = new ConstraintSet(ConstraintOp.AND);
                    // 0 Author
                    QueryClass qcAuthor = new QueryClass(Author.class);
                    qAuthor.addToSelect(qcAuthor);
                    qAuthor.addFrom(qcAuthor);
                    // 1 Author.publications
                    QueryClass qcPublication = new QueryClass(Publication.class);
                    qAuthor.addFrom(qcPublication);
                    qAuthor.addToSelect(qcPublication);
                    QueryCollectionReference authorPublications = new QueryCollectionReference(qcAuthor, "publications");
                    csAuthor.addConstraint(new ContainsConstraint(authorPublications, ConstraintOp.CONTAINS, qcPublication));
                    // constrain on this pub's original title (since title may have been updated above, e.g. initcaps to lower case)
                    QueryField qfPubTitle = new QueryField(qcPublication, "title");
                    SimpleConstraint scPubTitle = new SimpleConstraint(qfPubTitle, ConstraintOp.EQUALS, new QueryValue(origTitle));
                    csAuthor.addConstraint(scPubTitle);
                    qAuthor.setConstraint(csAuthor);
                    // put existing authors in a set for bulk deletion
                    Set<Author> authorSet = new HashSet<Author>();
                    Results authorResults = osw.getObjectStore().execute(qAuthor);
                    Iterator<?> authorIter = authorResults.iterator();
                    while (authorIter.hasNext()) {
                        ResultsRow<?> rr = (ResultsRow<?>) authorIter.next();
                        authorSet.add((Author)rr.get(0));
                    }
                    // delete them one by one (because bulk deletion is broken)
                    osw.beginTransaction();
                    for (Author author : authorSet) {
                        osw.delete(author);
                    }
                    osw.commitTransaction();

                    // place this pub's authors in a set to add to its authors collection; store the ones that are new
                    authorSet = new HashSet<Author>();
                    osw.beginTransaction();
                    for (Object authorObject : authors)  {
                        JSONObject authorJSON = (JSONObject) authorObject;
                        // IM Author attributes
                        String firstName = (String) authorJSON.get("given");
                        String lastName = (String) authorJSON.get("family");
                        String name = firstName+" "+lastName;
                        Author author;
                        if (authorMap.containsKey(name)) {
                            author = authorMap.get(name);
                        } else {
                            author = (Author) DynamicUtil.createObject(Collections.singleton(Author.class));
                            author.setFieldValue("firstName", firstName);
                            author.setFieldValue("lastName", lastName);
                            author.setFieldValue("name", name);
                            osw.store(author);
                            authorMap.put(name, author);
                        }
                        authorSet.add(author);
                        LOG.info(author.getFieldValue("name"));
                    }
                    osw.commitTransaction();

                    // put these authors into the pub authors collection
                    tempPub.setFieldValue("authors", authorSet);
                    
                }

                // store this publication
                osw.beginTransaction();
                osw.store(tempPub);
                osw.commitTransaction();
                
            } catch (Exception e) {
                LOG.error(e);
            }

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

}
