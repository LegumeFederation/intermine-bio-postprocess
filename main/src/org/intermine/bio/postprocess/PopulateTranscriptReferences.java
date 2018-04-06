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

import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.OrderDescending;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Create transcript references to genes (Transcript.gene) and proteins (Transcript.protein) by matching up primaryIdentifiers.
 *
 * @author Sam Hokin
 */
public class PopulateTranscriptReferences {

    private static final Logger LOG = Logger.getLogger(PopulateTranscriptReferences.class);
    protected ObjectStoreWriter osw;

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulateTranscriptReferences(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Run the analysis.
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void populateTranscriptReferences() throws ObjectStoreException, IllegalAccessException {

        LOG.info("Querying transcripts...");

        Query qT = new Query();
        QueryClass qcT = new QueryClass(Transcript.class);
        qT.addToSelect(qcT);
        qT.addFrom(qcT);
        Results results = osw.getObjectStore().execute(qT);
        Iterator<?> iter = results.iterator();
        Set<Transcript> transcripts = new HashSet<Transcript>();
        while (iter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) iter.next();
            transcripts.add((Transcript) rr.get(0));
        }

        osw.beginTransaction();
        
        for (Transcript transcript : transcripts) {
            
            String transcriptName = (String) transcript.getFieldValue("primaryIdentifier");
            String[] chunks = transcriptName.split("\\.");
            int n = chunks.length;
            
            // form gene name from transcript name
            String geneName = "";
            for (int i=0; i<n-1; i++) {
                if (i>0) geneName += ".";
                geneName += chunks[i];
            }

            // HACK - protein should have same ID as transcript, but does not currently for lupan
            // form protein ID from transcript ID
            String proteinName = chunks[n-2]+"."+chunks[n-1];
            
            // query the gene
            Query qG = new Query();
            QueryClass qcG = new QueryClass(Gene.class);
            qG.addToSelect(qcG);
            qG.addFrom(qcG);
            QueryField qfG = new QueryField(qcG, "primaryIdentifier");
            ConstraintSet csG = new ConstraintSet(ConstraintOp.AND);
            SimpleConstraint scG = new SimpleConstraint(qfG, ConstraintOp.EQUALS, new QueryValue(geneName));
            csG.addConstraint(scG);
            qG.setConstraint(csG);
            Results rG = osw.getObjectStore().execute(qG);
            Iterator<?> iterG = rG.iterator();
            Gene gene = null;
            if (iterG.hasNext()) {
                ResultsRow<?> rrG = (ResultsRow<?>) iterG.next();
                gene = (Gene) rrG.get(0);
            } else {
                LOG.info("Gene "+geneName+" not found for transcript "+transcriptName);
            }

            // query the protein
            Query qP = new Query();
            QueryClass qcP = new QueryClass(Protein.class);
            qP.addToSelect(qcP);
            qP.addFrom(qcP);
            QueryField qfP = new QueryField(qcP, "primaryIdentifier");
            ConstraintSet csP = new ConstraintSet(ConstraintOp.AND);
            SimpleConstraint scP = new SimpleConstraint(qfP, ConstraintOp.EQUALS, new QueryValue(proteinName));
            csP.addConstraint(scP);
            qP.setConstraint(csP);
            Results rP = osw.getObjectStore().execute(qP);
            Iterator<?> iterP = rP.iterator();
            Protein protein = null;
            if (iterP.hasNext()) {
                ResultsRow<?> rrP = (ResultsRow<?>) iterP.next();
                protein = (Protein) rrP.get(0);
            } else {
                LOG.info("Protein "+proteinName+" not found for transcript "+transcriptName);
            }
            
            // set references
            if (gene!=null || protein!=null) {
                Transcript tempTranscript = PostProcessUtil.cloneInterMineObject(transcript);
                if (gene!=null) tempTranscript.setFieldValue("gene", gene);
                if (protein!=null) tempTranscript.setFieldValue("protein", protein);
                osw.store(tempTranscript);
            }
            
        }

        osw.commitTransaction();
        
    }

}
