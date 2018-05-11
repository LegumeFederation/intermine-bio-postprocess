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

import org.intermine.model.bio.GeneticMarker;
import org.intermine.model.bio.LinkageGroup;
import org.intermine.model.bio.LinkageGroupPosition;
import org.intermine.model.bio.LinkageGroupRange;
import org.intermine.model.bio.QTL;

import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
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
 * Determine the linkage group and ranges of QTLs based on at least two associatd markers.
 *
 * Since the QTLs and genetic markers can be loaded from a variety of sources (flat files, chado), it makes sense to do this in post-processing when the 
 * QTLs and markers exist in the database in a standard format.
 *
 * @author Sam Hokin
 */
public class CreateQTLLinkageGroupReferences extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateQTLLinkageGroupReferences.class);

    static final DecimalFormat df = new DecimalFormat("#.00");

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public CreateQTLLinkageGroupReferences(ObjectStoreWriter osw) {
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
        
        // BUG: does not work! It should!
        // LOG.info("Deleting all existing LinkageGroupRange records...");
        // osw.beginTransaction();
        // QueryClass qcLGR = new QueryClass(LinkageGroupRange.class);
        // osw.delete(qcLGR, null);
        // osw.commitTransaction();

        // delete existing LinkageGroupRange objects by first loading them into a collection
        LOG.info("Deleting existing LinkageGroupRange records...");
        Set<LinkageGroupRange> lgrSet = new HashSet<LinkageGroupRange>();
        Query qLGR = new Query();
        QueryClass qcLGR = new QueryClass(LinkageGroupRange.class);
        qLGR.addToSelect(qcLGR);
        qLGR.addFrom(qcLGR);
        Results lgrResults = osw.getObjectStore().execute(qLGR);
        Iterator<?> lgrIter = lgrResults.iterator();
        osw.beginTransaction();
        while (lgrIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) lgrIter.next();
            lgrSet.add((LinkageGroupRange)rr.get(0));
        }
        osw.commitTransaction();
        // and then deleting them
        osw.beginTransaction();
        for (LinkageGroupRange lgr : lgrSet) {
            osw.delete(lgr);
        }
        osw.commitTransaction();

        LOG.info("Accumulating QTLs and linkage group/ranges from associated markers...");
        
        Query qQTL = new Query();
        qQTL.setDistinct(true);
        ConstraintSet csQTL = new ConstraintSet(ConstraintOp.AND);

        // 0 QTL
        QueryClass qcQTL = new QueryClass(QTL.class);
        qQTL.addToSelect(qcQTL);
        qQTL.addFrom(qcQTL);

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

        // results order
        QueryField qfQTL = new QueryField(qcQTL, "primaryIdentifier");
        qQTL.addToOrderBy(qfQTL);
        QueryField qfLinkageGroup = new QueryField(qcLinkageGroup, "primaryIdentifier");
        qQTL.addToOrderBy(qfLinkageGroup);
        QueryField qfLinkageGroupPosition = new QueryField(qcLinkageGroupPosition, "position");
        qQTL.addToOrderBy(qfLinkageGroupPosition);
        
        // set the constraints
        qQTL.setConstraint(csQTL);

        // execute the query
        Results qtlResults = osw.getObjectStore().execute(qQTL);
        Iterator<?> qtlIter = qtlResults.iterator();

        QTL lastQTL = null;
        String lastQTLId = "";
        LinkageGroup lastLG = null;
        String lastLGId = "";
        
        String startMarkerId = null;
        String endMarkerId = null;
        double startPos = 0.0;
        double endPos = 0.0;

        int qtlCount = 0;
        int lgCount = 0;
        int markerCount = 0;

        boolean newQTL = true;
        boolean newLG = true;
        
        // begin transaction
        osw.beginTransaction();
        
        while (qtlIter.hasNext()) {
            try {
                ResultsRow<?> rr = (ResultsRow<?>) qtlIter.next();
                // objects
                QTL qtl = (QTL) rr.get(0);
                GeneticMarker gm = (GeneticMarker) rr.get(1);
                LinkageGroupPosition lgp = (LinkageGroupPosition) rr.get(2);
                LinkageGroup lg = (LinkageGroup) rr.get(3);
                // field values
                String qtlId = (String) qtl.getFieldValue("primaryIdentifier");
                String gmId = (String) gm.getFieldValue("primaryIdentifier");
                String lgId = (String) lg.getFieldValue("primaryIdentifier");
                double position = (Double) lgp.getFieldValue("position");
                // logic
                newQTL = (!qtlId.equals(lastQTLId));
                newLG = (!lgId.equals(lastLGId));
                if ((newQTL || newLG) && markerCount>1 && endPos>startPos) {
                    // store last linkage group range IF it has nonzero length
                    QTL tempQTL = PostProcessUtil.cloneInterMineObject(lastQTL);
                    LinkageGroup tempLG = PostProcessUtil.cloneInterMineObject(lastLG);
                    LinkageGroupRange lgr = (LinkageGroupRange) DynamicUtil.createObject(Collections.singleton(LinkageGroupRange.class));
                    lgr.setFieldValue("begin", startPos);
                    lgr.setFieldValue("end", endPos);
                    lgr.setFieldValue("length", Double.parseDouble(df.format((endPos-startPos))));
                    lgr.setFieldValue("linkageGroup", tempLG);
                    // add linkage group range to QTLs collection
                    Set<LinkageGroupRange> lgrColl = new HashSet<LinkageGroupRange>();
                    lgrColl.add(lgr);
                    tempQTL.setFieldValue("linkageGroupRanges", lgrColl);
                    // add QTL to linkage group's collection (hopefully not dupe)
                    Set<QTL> qtlColl = new HashSet<QTL>();
                    qtlColl.add(tempQTL);
                    tempLG.setFieldValue("QTLs", qtlColl);
                    // store 'em
                    osw.store(lastLG);
                    osw.store(lgr);
                    osw.store(tempQTL);
                    osw.store(tempLG);
                }
                if (newQTL) {
                    // new QTL, increment QTL count, reset LG and marker counts and set start marker
                    qtlCount++;
                    lgCount = 1;
                    markerCount = 1;
                    startPos = position;
                    startMarkerId = gmId;
                } else if (newLG) {
                    // new LG, increment LG count, reset marker count, set start marker
                    lgCount++;
                    markerCount = 1;
                    startPos = position;
                    startMarkerId = gmId;
                } else {
                    // same QTL and LG, bump end position
                    markerCount++;
                    endPos = position;
                    endMarkerId = gmId;
                }
                lastQTL = qtl;
                lastQTLId = qtlId;
                lastLG = lg;
                lastLGId = lgId;
            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            }
        }

        // last one:
        if (markerCount>1) {
            try {
                // store last linkage group range
                QTL tempQTL = PostProcessUtil.cloneInterMineObject(lastQTL);
                LinkageGroup tempLG = PostProcessUtil.cloneInterMineObject(lastLG);
                LinkageGroupRange lgr = (LinkageGroupRange) DynamicUtil.createObject(Collections.singleton(LinkageGroupRange.class));
                lgr.setFieldValue("begin", startPos);
                lgr.setFieldValue("end", endPos);
                lgr.setFieldValue("length", Double.parseDouble(df.format((endPos-startPos))));
                lgr.setFieldValue("linkageGroup", tempLG);
                // add linkage group range to QTLs collection
                Set<LinkageGroupRange> lgrColl = new HashSet<LinkageGroupRange>();
                lgrColl.add(lgr);
                tempQTL.setFieldValue("linkageGroupRanges", lgrColl);
                // add QTL to linkage group's collection (hopefully not dupe)
                Set<QTL> qtlColl = new HashSet<QTL>();
                qtlColl.add(tempQTL);
                tempLG.setFieldValue("QTLs", qtlColl);
                // store 'em
                osw.store(lastLG);
                osw.store(lgr);
                osw.store(tempQTL);
                osw.store(tempLG);
            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            }
        }

        // commit and close transaction
        osw.commitTransaction();

    }

}
