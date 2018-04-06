package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2002-2016 FlyMine, Legume Federation
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
import java.util.Set;
import java.util.HashSet;

import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.LinkageGroup;
import org.intermine.model.bio.LinkageGroupPosition;

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
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Determine the linkage group length based on the largest position of its genetic markers.
 *
 * @author Sam Hokin
 */
public class PopulateLinkageGroupLengths {

    private static final Logger LOG = Logger.getLogger(PopulateLinkageGroupLengths.class);
    protected ObjectStoreWriter osw;

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulateLinkageGroupLengths(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Run the analysis.
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void populateLinkageGroupLengths() throws ObjectStoreException, IllegalAccessException {

        Set<String> lgWithLengths = new HashSet<String>();

        LOG.info("Querying existing linkage group lengths...");

        Query qLG = new Query();
        qLG.setDistinct(true);
        QueryClass qcLG = new QueryClass(LinkageGroup.class);
        qLG.addToSelect(qcLG);
        qLG.addFrom(qcLG);
        Results lgResults = osw.getObjectStore().execute(qLG);
        Iterator<?> lgIter = lgResults.iterator();
        while (lgIter.hasNext()) {
            ResultsRow<?> rrLG = (ResultsRow<?>) lgIter.next();
            LinkageGroup lg = (LinkageGroup) rrLG.get(0);
            String primaryIdentifier = (String) lg.getFieldValue("primaryIdentifier");
            Double length = (Double) lg.getFieldValue("length");
            if (length!=null) {
                lgWithLengths.add(primaryIdentifier);
            }
        }
        
        LOG.info("Querying linkage groups and associated markers...");
        
        Query qLGP = new Query();
        qLGP.setDistinct(true);
        ConstraintSet csLGP = new ConstraintSet(ConstraintOp.AND);

        // 0 LinkageGroupPosition
        QueryClass qcLGP = new QueryClass(LinkageGroupPosition.class);
        qLGP.addToSelect(qcLGP);
        qLGP.addFrom(qcLGP);

        // 1 LinkageGroupPosition.linkageGroup
        qcLG = new QueryClass(LinkageGroup.class);
        qLGP.addToSelect(qcLG);
        qLGP.addFrom(qcLG);
        QueryObjectReference linkageGroup = new QueryObjectReference(qcLGP, "linkageGroup");
        csLGP.addConstraint(new ContainsConstraint(linkageGroup, ConstraintOp.CONTAINS, qcLG));

        // results order
        QueryField qfLG = new QueryField(qcLG, "primaryIdentifier");
        qLGP.addToOrderBy(qfLG);
        OrderDescending odLGP = new OrderDescending(new QueryField(qcLGP, "position"));
        qLGP.addToOrderBy(odLGP);
        
        // set the constraints
        qLGP.setConstraint(csLGP);

        // keep track of the previous linkage group ID
        String prevLGId = "";
        
        // execute the query
        Results lgpResults = osw.getObjectStore().execute(qLGP);
        Iterator<?> lgpIter = lgpResults.iterator();
        // begin transaction
        osw.beginTransaction();
        while (lgpIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) lgpIter.next();
            // objects
            LinkageGroupPosition lgp = (LinkageGroupPosition) rr.get(0);
            LinkageGroup lg = (LinkageGroup) rr.get(1);
            // field values
            double position = (Double) lgp.getFieldValue("position");
            String lgId = (String) lg.getFieldValue("primaryIdentifier");
            // logic
            boolean newLG = (!lgId.equals(prevLGId));
            if (newLG) {
                // store this position, which should be the largest from sort order, as LG length
                if (position>0.0) {
                    if (lgWithLengths.contains(lgId)) {
                        LOG.info(lgId+" already contains length and is NOT being updated.");
                    } else {
                        LinkageGroup tempLG = PostProcessUtil.cloneInterMineObject(lg);
                        tempLG.setFieldValue("length", position);
                        osw.store(tempLG);
                        LOG.info(lgId+" updated with length="+position);
                    }
                }
                prevLGId = lgId;
            }
        }

        // commit and close transaction
        osw.commitTransaction();

    }

}
