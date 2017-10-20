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
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.apache.log4j.Logger;

/**
 * Find genes which are spanned by the genomic range of markers associated with QTLs when there are at least two (often three: nearest, flanking left and right).
 * Note: single gene-marker associations are not done here since they are found automatically as overlapping features.
 *
 * Since the QTLs and genetic markers can be loaded from a variety of sources (flat files, chado), it makes sense to do this in post-processing when the 
 * QTLs, markers and genes exist in the database in a standard format.
 *
 * @author Sam Hokin
 */
public class PopulateGeneSpanningQTLs {

    private static final Logger LOG = Logger.getLogger(PopulateGeneSpanningQTLs.class);

    protected ObjectStoreWriter osw;

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

        // 1 QTL.associatedGeneticMarkers
        QueryClass qcGeneticMarker = new QueryClass(GeneticMarker.class);
        qQTL.addToSelect(qcGeneticMarker);
        qQTL.addFrom(qcGeneticMarker);
        QueryCollectionReference qtlGeneticMarkers = new QueryCollectionReference(qcQTL, "associatedGeneticMarkers");
        csQTL.addConstraint(new ContainsConstraint(qtlGeneticMarkers, ConstraintOp.CONTAINS, qcGeneticMarker));

        // 2 GeneticMarker.chromosome
        QueryClass qcChromosome = new QueryClass(Chromosome.class);
        qQTL.addToSelect(qcChromosome);
        qQTL.addFrom(qcChromosome);
        QueryObjectReference gmChromosome = new QueryObjectReference(qcGeneticMarker, "chromosome");
        csQTL.addConstraint(new ContainsConstraint(gmChromosome, ConstraintOp.CONTAINS, qcChromosome));

        // 3 GeneticMarker.chromosomeLocation
        QueryClass qcLocation = new QueryClass(Location.class);
        qQTL.addToSelect(qcLocation);
        qQTL.addFrom(qcLocation);
        QueryObjectReference gmLocation = new QueryObjectReference(qcGeneticMarker, "chromosomeLocation");
        csQTL.addConstraint(new ContainsConstraint(gmLocation, ConstraintOp.CONTAINS, qcLocation));

        // results order
        QueryField qfQTL = new QueryField(qcQTL, "primaryIdentifier");
        qQTL.addToOrderBy(qfQTL);
        QueryField qfChromosome = new QueryField(qcChromosome, "primaryIdentifier");
        qQTL.addToOrderBy(qfChromosome);
        QueryField qfStart = new QueryField(qcLocation, "start");
        qQTL.addToOrderBy(qfStart);
        QueryField qfEnd = new QueryField(qcLocation, "end");
        qQTL.addToOrderBy(qfEnd);

        // set the constraints
        qQTL.setConstraint(csQTL);

        // initialize some outside vars
        QTL lastQTL = null;
        Chromosome lastChr = null;
        
        String lastQTLId = "";
        String lastChrId = "";

        int startLoc = 0;
        int endLoc = 0;

        int qtlCount = 0;
        int chrCount = 0;
        int markerCount = 0;

        boolean newQTL = true;
        boolean newChr = true;

        // we'll store our QTLs and genomic span in a set for comparison when we drill through the genes
        Set<QTLSpan> qtlSpanSet = new HashSet<QTLSpan>();

        // execute the query
        Iterator<?> qtlIter = osw.getObjectStore().execute(qQTL).iterator();
        while (qtlIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) qtlIter.next();
            // objects
            QTL qtl = (QTL) rr.get(0);
            String qtlId = (String) qtl.getFieldValue("primaryIdentifier");
            GeneticMarker gm = (GeneticMarker) rr.get(1);
            String gmId = (String) gm.getFieldValue("primaryIdentifier");
            Chromosome chr = (Chromosome) rr.get(2);
            String chrId = (String) chr.getFieldValue("primaryIdentifier");
            Location loc = (Location) rr.get(3);
            int start = ((Integer) loc.getFieldValue("start")).intValue();
            int end = ((Integer) loc.getFieldValue("end")).intValue();
            // logic
            newQTL = (!qtlId.equals(lastQTLId));
            newChr = (!chrId.equals(lastChrId));
            if ((newQTL || newChr) && markerCount>1 && startLoc>0 && startLoc<endLoc) {
                // store last QTL span
                qtlSpanSet.add(new QTLSpan(lastQTL, lastChrId, startLoc, endLoc));
                LOG.info(lastQTLId+" "+lastChrId+":"+startLoc+"-"+endLoc+" ("+markerCount+" markers)");
            }
            if (newQTL) {
                // new QTL, increment QTL count, reset Chr and marker counts and set start marker (HOPING THEY'RE ORDERED BY START!!!)
                qtlCount++;
                chrCount = 1;
                markerCount = 1;
                startLoc = start;
                endLoc = end;
            } else if (newChr) {
                // new Chr, increment Chr count, reset marker count, set start marker
                chrCount++;
                markerCount = 1;
                startLoc = start;
                endLoc = end;
            } else {
                // same QTL and Chr, bump end value IF larger and different marker
                boolean sameMarker = (start==startLoc && end==endLoc);
                if (!sameMarker && end>endLoc) {
                    markerCount++;
                    endLoc = end;
                }
            }
            // set "last" values for next iteration
            lastQTL = qtl;
            lastQTLId = qtlId;
            lastChr = chr;
            lastChrId = chrId;
        }
        // last one:
        if (startLoc>endLoc) {
            // store last QTL span
            qtlSpanSet.add(new QTLSpan(lastQTL, lastChrId, startLoc, endLoc));
            LOG.info(lastQTLId+" "+lastChrId+":"+startLoc+"-"+endLoc);
        }
        LOG.info("qtlSpanSet.size()="+qtlSpanSet.size());

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

        // 1 Gene.chromosome
        QueryClass qcGeneChromosome = new QueryClass(Chromosome.class);
        qGene.addFrom(qcGeneChromosome);
        qGene.addToSelect(qcGeneChromosome);
        QueryObjectReference geneChromosome = new QueryObjectReference(qcGene, "chromosome");
        csGene.addConstraint(new ContainsConstraint(geneChromosome, ConstraintOp.CONTAINS, qcGeneChromosome));

        // 2 Gene.chromosomeLocation
        QueryClass qcGeneLocation = new QueryClass(Location.class);
        qGene.addFrom(qcGeneLocation);
        qGene.addToSelect(qcGeneLocation);
        QueryObjectReference geneLocation = new QueryObjectReference(qcGene, "chromosomeLocation");
        csGene.addConstraint(new ContainsConstraint(geneLocation, ConstraintOp.CONTAINS, qcGeneLocation));

        // sort order
        QueryField qfGene = new QueryField(qcGene, "primaryIdentifier");
        qGene.addToOrderBy(qfGene);

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
                if (qtlSpan.chromosomeId.equals(chrId) && qtlSpan.end>=start && qtlSpan.start<=end) {
		    osw.store(qtlSpan.qtl);
                    qtlCollection.add(qtlSpan.qtl);
                }
            }
            // now add the collection to the gene if it is spanned by QTLs
            if (qtlCollection.size()>0) {
                Gene tempGene = PostProcessUtil.cloneInterMineObject(gene);
                tempGene.setFieldValue("spanningQTLs", qtlCollection);
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
