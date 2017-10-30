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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.GeneticMarker;
import org.intermine.model.bio.LinkageGroup;
import org.intermine.model.bio.LinkageGroupPosition;
import org.intermine.model.bio.LinkageGroupRange;
import org.intermine.model.bio.QTL;

import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Determine the linkage group ranges of QTLs based on at least two associatd markers.
 *
 * Since the QTLs and genetic markers can be loaded from a variety of sources (flat files, chado), it makes sense to do this in post-processing when the 
 * QTLs and markers exist in the database in a standard format.
 *
 * @author Sam Hokin
 */
public class PopulateQTLLinkageGroupRanges {

    private static final Logger LOG = Logger.getLogger(PopulateQTLLinkageGroupRanges.class);
    protected ObjectStoreWriter osw;

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulateQTLLinkageGroupRanges(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Find the spanned genes for QTLs+markers.
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void populateQTLLinkageGroupRanges() throws ObjectStoreException, IllegalAccessException {
        
        // DEBUG: does not work! It should!
        // LOG.info("Deleting all existing LinkageGroupRange records...");
        // osw.beginTransaction();
        // QueryClass qcLGR = new QueryClass(LinkageGroupRange.class);
        // osw.delete(qcLGR, null);
        // osw.commitTransaction();
        
        LOG.info("Accumulating QTLs and linkage group ranges from associated markers...");
        
        Query qQTL = new Query();
        qQTL.setDistinct(true);
        ConstraintSet csQTL = new ConstraintSet(ConstraintOp.AND);

        // 0 QTL
        QueryClass qcQTL = new QueryClass(QTL.class);
        qQTL.addToSelect(qcQTL);
        qQTL.addFrom(qcQTL);
        qQTL.addToOrderBy(qcQTL);

        // 1 QTL.associatedGeneticMarkers
        QueryClass qcGeneticMarker = new QueryClass(GeneticMarker.class);
        qQTL.addFrom(qcGeneticMarker);
        qQTL.addToSelect(qcGeneticMarker);
        QueryCollectionReference qtlGeneticMarkers = new QueryCollectionReference(qcQTL, "associatedGeneticMarkers");
        csQTL.addConstraint(new ContainsConstraint(qtlGeneticMarkers, ConstraintOp.CONTAINS, qcGeneticMarker));

        // 2 GeneticMarker.linkageGroupPositions
        QueryClass qcLinkageGroupPosition = new QueryClass(LinkageGroupPosition.class);
        qQTL.addFrom(qcLinkageGroupPosition);
        qQTL.addToSelect(qcLinkageGroupPosition);
        QueryCollectionReference gmLinkageGroupPositions = new QueryCollectionReference(qcGeneticMarker, "linkageGroupPositions");
        csQTL.addConstraint(new ContainsConstraint(gmLinkageGroupPositions, ConstraintOp.CONTAINS, qcLinkageGroupPosition));

        // 3 GeneticMarker.linkageGroupPositions.linkageGroup
        QueryClass qcLinkageGroup = new QueryClass(LinkageGroup.class);
        qQTL.addFrom(qcLinkageGroup);
        qQTL.addToSelect(qcLinkageGroup);
        QueryObjectReference gmLinkageGroup = new QueryObjectReference(qcLinkageGroupPosition, "linkageGroup");
        csQTL.addConstraint(new ContainsConstraint(gmLinkageGroup, ConstraintOp.CONTAINS, qcLinkageGroup));

        // set the constraints
        qQTL.setConstraint(csQTL);

        // execute the query
        Results qtlResults = osw.getObjectStore().execute(qQTL);
        Iterator<?> qtlIter = qtlResults.iterator();

        // begin transaction
        osw.beginTransaction();

        QTL lastQTL = null;
        String lastQTLId = "";
        String lastLGId = "";
        String startMarkerId = null;
        String endMarkerId = null;
        double minStart = 1000000.0;
        double maxEnd = 0.0;
        int qtlCount = 0;
        boolean singleLG = true;

        while (qtlIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) qtlIter.next();
            // objects
            QTL qtl = (QTL) rr.get(0);
            GeneticMarker gm = (GeneticMarker) rr.get(1);
            LinkageGroupPosition lgp = (LinkageGroupPosition) rr.get(2);
            LinkageGroup lg = (LinkageGroup)  rr.get(3);
            // field values
            String qtlId = (String) qtl.getFieldValue("primaryIdentifier");
            String gmId = (String) gm.getFieldValue("primaryIdentifier");
            String lgId = (String) lg.getFieldValue("primaryIdentifier");
            double position = (Double) lgp.getFieldValue("position");
            
            // logic
            if (qtlId.equals(lastQTLId)) {
                qtlCount++;
                if (lgId.equals(lastLGId)) {
                    if (position<minStart) {
                        minStart = position;
                        startMarkerId = gmId;
                    }
                    if (position>maxEnd) {
                        maxEnd = position;
                        endMarkerId = gmId;
                    }
                } else {
                    singleLG = false;
                }
            } else {
                // only update QTLs with more than one marker, on a single linkage group, so there is a well-defined genetic range
                if (maxEnd>0.0 && qtlCount>1 && singleLG) {
                    QTL tempQTL = PostProcessUtil.cloneInterMineObject(lastQTL);
                    LinkageGroupRange lgr = (LinkageGroupRange) DynamicUtil.createObject(Collections.singleton(LinkageGroupRange.class));
                    lgr.setFieldValue("begin", minStart);
                    lgr.setFieldValue("end", maxEnd);
                    lgr.setFieldValue("length", (maxEnd-minStart));
                    lgr.setFieldValue("linkageGroup", lg);
                    Set<LinkageGroupRange> lgrCollection = new HashSet<LinkageGroupRange>();
                    lgrCollection.add(lgr);
                    tempQTL.setFieldValue("linkageGroupRanges", lgrCollection);
                    osw.store(lgr);
                    osw.store(tempQTL);
                }
                // set new lastQTL
                lastQTL = qtl;
                lastQTLId = qtlId;
                lastLGId = lgId;
                minStart = position;
                maxEnd = position;
                startMarkerId = gmId;
                endMarkerId = gmId;
                qtlCount = 1;
                singleLG = true;
            }

        }

        // last one:
        if (maxEnd>0.0 && qtlCount>1 && singleLG) {
            QTL tempQTL = PostProcessUtil.cloneInterMineObject(lastQTL);
            LinkageGroupRange lgr = (LinkageGroupRange) DynamicUtil.createObject(Collections.singleton(LinkageGroupRange.class));
            lgr.setFieldValue("begin", minStart);
            lgr.setFieldValue("end", maxEnd);
            Set<LinkageGroupRange> lgrCollection = new HashSet<LinkageGroupRange>();
            lgrCollection.add(lgr);
            tempQTL.setFieldValue("linkageGroupRanges", lgrCollection);
            osw.store(lgr);
            osw.store(tempQTL);
        }

        // commit and close transaction
        osw.commitTransaction();

    }

}
