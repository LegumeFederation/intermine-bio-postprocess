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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.intermine.metadata.ConstraintOp;
import org.intermine.model.bio.Chromosome;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GeneticMarker;
import org.intermine.model.bio.Location;
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

import org.apache.log4j.Logger;

/**
 * Find genes which are spanned by the genomic range of markers associated with QTLs when there are at least two (usually three: nearest, flanking left and right).
 *
 * Since the QTLs and genetic markers can be loaded from a variety of sources (flat files, chado), it makes sense to do this in post-processing when the 
 * QTLs, markers and genes exist in the database in a standard format.
 *
 * @author Sam Hokin
 */
public class PopulateGeneSpanningQTLs {

    private static final Logger LOG = Logger.getLogger(PopulateGeneSpanningQTLs.class);
    protected ObjectStoreWriter osw;

    private static final String QTL_COLLECTION = "spanningQTLs";

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulateGeneSpanningQTLs(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Find the spanned genes for QTLs+markers.
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void populateGeneSpanningQTLs() throws ObjectStoreException, IllegalAccessException {
        
        // ----------------------------------------------------------------------------------------------
        // First section - accumulate the QTLs and their genomic spans, from their associated markers
        // ----------------------------------------------------------------------------------------------

        LOG.info("Accumulating QTLs and genomic spans from associated markers...");
        
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

        // 2 GeneticMarker.chromosome
        QueryClass qcChromosome = new QueryClass(Chromosome.class);
        qQTL.addFrom(qcChromosome);
        qQTL.addToSelect(qcChromosome);
        QueryObjectReference gmChromosome = new QueryObjectReference(qcGeneticMarker, "chromosome");
        csQTL.addConstraint(new ContainsConstraint(gmChromosome, ConstraintOp.CONTAINS, qcChromosome));

        // 3 GeneticMarker.chromosomeLocation
        QueryClass qcLocation = new QueryClass(Location.class);
        qQTL.addFrom(qcLocation);
        qQTL.addToSelect(qcLocation);
        QueryObjectReference gmLocation = new QueryObjectReference(qcGeneticMarker, "chromosomeLocation");
        csQTL.addConstraint(new ContainsConstraint(gmLocation, ConstraintOp.CONTAINS, qcLocation));

        // set the constraints
        qQTL.setConstraint(csQTL);

        // execute the query
        Results qtlResults = osw.getObjectStore().execute(qQTL);
        Iterator<?> qtlIter = qtlResults.iterator();

        // we'll store our QTLs and genomic span in a set for comparison when we drill through the genes
        Set<QTLSpan> qtlSpanSet = new HashSet<QTLSpan>();
        
        QTL lastQTL = null;
        String lastQTLId = "";
        String lastChrId = "";
        String startMarkerId = null;
        String endMarkerId = null;
        int minStart = 100000000;
        int maxEnd = 0;
        int qtlCount = 0;
        boolean singleChr = true;

        while (qtlIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) qtlIter.next();
            // objects
            QTL qtl = (QTL) rr.get(0);
            GeneticMarker gm = (GeneticMarker) rr.get(1);
            Chromosome chr = (Chromosome) rr.get(2);
            Location loc = (Location) rr.get(3);
            // field values
            String qtlId = (String) qtl.getFieldValue("primaryIdentifier");
            String gmId = (String) gm.getFieldValue("primaryIdentifier");
            String chrId = (String) chr.getFieldValue("primaryIdentifier");
            int start = ((Integer) loc.getFieldValue("start")).intValue();
            int end = ((Integer) loc.getFieldValue("end")).intValue();
            // logic
            if (qtlId.equals(lastQTLId)) {
                qtlCount++;
                if (chrId.equals(lastChrId)) {
                    if (start<minStart) {
                        minStart = start;
                        startMarkerId = gmId;
                    }
                    if (end>maxEnd) {
                        maxEnd = end;
                        endMarkerId = gmId;
                    }
                } else {
                    singleChr = false;
                }
            } else {
                // only store QTLs with more than one marker, on a single chromosome, so there is a well-defined genomic range
                if (maxEnd>0 && qtlCount>1 && singleChr) {
                    qtlSpanSet.add(new QTLSpan(lastQTL, lastChrId, minStart, maxEnd));
                }
                lastQTL = qtl;
                lastQTLId = qtlId;
                lastChrId = chrId;
                minStart = start;
                maxEnd = end;
                startMarkerId = gmId;
                endMarkerId = gmId;
                qtlCount = 1;
                singleChr = true;
            }
        }
        // last one:
        if (maxEnd>0 && qtlCount>1 && singleChr) {
            qtlSpanSet.add(new QTLSpan(lastQTL, lastChrId, minStart, maxEnd));
        }

        // ------------------------------------------------------------------------------------------------------------------------------
        // Second section: spin through the genes, comparing their genomic range to the QTL genomic spans, associate them if overlapping
        // ------------------------------------------------------------------------------------------------------------------------------

        LOG.info("Spinning through genes, associating with QTL if spanned by QTL genomic range...");

        Query qGene = new Query();
        qGene.setDistinct(false);
        ConstraintSet csGene = new ConstraintSet(ConstraintOp.AND);

        // 0 Gene
        QueryClass qcGene = new QueryClass(Gene.class);
        qGene.addFrom(qcGene);
        qGene.addToSelect(qcGene);
        qGene.addToOrderBy(qcGene);

        // 1 Gene.chromosome
        QueryClass qcGeneChromosome = new QueryClass(Chromosome.class);
        qGene.addFrom(qcGeneChromosome);
        qGene.addToSelect(qcGeneChromosome);
        qGene.addToOrderBy(qcGeneChromosome);
        QueryObjectReference geneChromosome = new QueryObjectReference(qcGene, "chromosome");
        csGene.addConstraint(new ContainsConstraint(geneChromosome, ConstraintOp.CONTAINS, qcGeneChromosome));

        // 2 Gene.chromosomeLocation
        QueryClass qcGeneLocation = new QueryClass(Location.class);
        qGene.addFrom(qcGeneLocation);
        qGene.addToSelect(qcGeneLocation);
        qGene.addToOrderBy(qcGeneLocation);
        QueryObjectReference geneLocation = new QueryObjectReference(qcGene, "chromosomeLocation");
        csGene.addConstraint(new ContainsConstraint(geneLocation, ConstraintOp.CONTAINS, qcGeneLocation));

        // set the overall constraint
        qGene.setConstraint(csGene);

        // execute the query
        Results geneResults = osw.getObjectStore().execute(qGene);
        Iterator<?> geneIter = geneResults.iterator();

        // begin transaction
        osw.beginTransaction();
        
        while (geneIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) geneIter.next();
            // objects
            Gene gene = (Gene) rr.get(0);
            Chromosome chr = (Chromosome) rr.get(1);
            Location loc = (Location) rr.get(2);
            // field values
            String geneId = (String) gene.getFieldValue("primaryIdentifier");
            String chrId = (String) chr.getFieldValue("primaryIdentifier");
            int start = ((Integer) loc.getFieldValue("start")).intValue();
            int end = ((Integer) loc.getFieldValue("end")).intValue();
            // loop through QTLSpans, adding QTLs to a set when gene is spanned by QTL
            Set<QTL> qtlCollection = new HashSet<QTL>();
            for (QTLSpan qtlSpan : qtlSpanSet) {
                if (chrId.equals(qtlSpan.chromosomeId) && start<=qtlSpan.end && end>=qtlSpan.start) {
                    String qtlId = (String) qtlSpan.qtl.getFieldValue("primaryIdentifier");
                    qtlCollection.add(qtlSpan.qtl);
                }
            }
            // now add the collection to the gene if it is spanned by QTLs
            if (qtlCollection.size()>0) {
                Gene tempGene = PostProcessUtil.cloneInterMineObject(gene);
                tempGene.setFieldValue(QTL_COLLECTION, qtlCollection);
                osw.store(tempGene);
            }
        }
        
        // close transaction
        osw.commitTransaction();

    }

    /**
     * Encapsulates a QTL and the genomic span: chromosome, start and end. Only need chromosome ID.
     */
    private class QTLSpan {

        QTL qtl;
        String chromosomeId;
        int start;
        int end;

        QTLSpan(QTL qtl, String chromosomeId, int start, int end) {
            this.qtl = qtl;
            this.chromosomeId = chromosomeId;
            this.start = start;
            this.end = end;
        }

    }
        
}
