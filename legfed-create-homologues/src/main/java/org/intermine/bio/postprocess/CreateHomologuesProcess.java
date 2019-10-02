package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2019 NCGR
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.bio.util.BioQueries;
import org.intermine.metadata.ClassDescriptor;
import org.intermine.metadata.MetaDataException;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GeneFamily;
import org.intermine.model.bio.Homologue;
import org.intermine.model.bio.Protein;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Create homologue records, each of which ties a gene to another gene in the same gene family.
 *
 * @author Sam Hokin
 */
public class CreateHomologuesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateHomologuesProcess.class);
    private ObjectStore os;

    /**
     * Create a new CreateHomologuesProcess object that will operate on the given
     * ObjectStoreWriter.
     *
     * @param osw the ObjectStoreWriter to use when creating/changing objects
     */
    public CreateHomologuesProcess(ObjectStoreWriter osw) {
        super(osw);
        this.os = osw.getObjectStore();
    }

    /**
     * {@inheritDoc}
     *
     * Main method
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void postProcess() throws ObjectStoreException {

        LOG.info("Deleting existing Homologues...");

        // delete existing Homologue objects by first loading them into a collection...
        List<Homologue> homoList = new LinkedList<Homologue>();
        Query qHomo = new Query();
        QueryClass qcHomo = new QueryClass(Homologue.class);
        qHomo.addToSelect(qcHomo);
        qHomo.addFrom(qcHomo);
        Results homoResults = os.execute(qHomo);
        Iterator<?> homoIter = homoResults.iterator();
        while (homoIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) homoIter.next();
            homoList.add((Homologue)rr.get(0));
        }
        // ...and then deleting them
        osw.beginTransaction();
        for (Homologue homo : homoList) {
            osw.delete(homo);
        }
        osw.commitTransaction();

        // now run through the gene families, creating a gene.homologues Homologue for each gene pair
        List<GeneFamily> gfList = new LinkedList<GeneFamily>();
        Query gfQuery = new Query();
        QueryClass gfQueryClass = new QueryClass(GeneFamily.class);
        gfQuery.addToSelect(gfQueryClass);
        gfQuery.addFrom(gfQueryClass);
        Results gfResults = os.execute(gfQuery);
        Iterator<?> gfIter = gfResults.iterator();
        while (gfIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) gfIter.next();
            GeneFamily gf = (GeneFamily)rr.get(0);
            List<Gene> genes = new LinkedList<>();
            for (Protein protein : gf.getProteins()) {
                for (Gene gene : protein.getGenes()) {
                    genes.add(gene);
                }
            }
        }
    }

    // private void createAndStoreFlankingRegion(Chromosome chr, Location geneLoc, Gene gene) throws ObjectStoreException {
	
    //     // This code can't cope with chromosomes that don't have a length
    //     if (chr.getLength() == null) {
    //         //LOG.warn("Attempted to create Homologues on a chromosome without a length: " + chr.getPrimaryIdentifier());
    //         return;
    //     }

    //     for (double distance : distances) {
    //         for (String direction : directions) {
    //             for (boolean includeGene : includeGenes) {
    //                 String strand = geneLoc.getStrand();

    //                 // TODO what do we do if strand not set?
    //                 int geneStart = geneLoc.getStart().intValue();
    //                 int geneEnd = geneLoc.getEnd().intValue();
    //                 int chrLength = chr.getLength().intValue();

    //                 // gene touches a chromosome end so there isn't a flanking region
    //                 if ((geneStart <= 1) || (geneEnd >= chrLength)) {
    //                     continue;
    //                 }

    //                 Homologue region = (Homologue) DynamicUtil.createObject(Collections.singleton(Homologue.class));
    //                 Location location = (Location) DynamicUtil.createObject(Collections.singleton(Location.class));

    //                 region.setDistance("" + distance + "kb");
    //                 region.setDirection(direction);
    //                 try {
    //                     PostProcessUtil.checkFieldExists(os.getModel(), "Homologue", "includeGene", "Not setting");
    //                     region.setFieldValue("includeGene", Boolean.valueOf(includeGene));
    //                 } catch (MetaDataException e) {
    //                     // Homologue.includeGene not in model so do nothing
    //                 }
    //                 region.setGene(gene);
    //                 region.setChromosome(chr);
    //                 region.setChromosomeLocation(location);
    //                 region.setOrganism(gene.getOrganism());
    //                 region.setPrimaryIdentifier(gene.getPrimaryIdentifier() + " " + distance + "kb " + direction);

    //                 // this should be some clever algorithm
    //                 int start, end;

    //                 if ("upstream".equals(direction) && "1".equals(strand)) {
    //                     start = geneStart - (int) Math.round(distance * 1000);
    //                     end = includeGene ? geneEnd : geneStart - 1;
    //                 } else if ("upstream".equals(direction) && "-1".equals(strand)) {
    //                     start = includeGene ? geneStart : geneEnd + 1;
    //                     end = geneEnd + (int) Math.round(distance * 1000);
    //                 } else if ("downstream".equals(direction) && "1".equals(strand)) {
    //                     start = includeGene ? geneStart : geneEnd + 1;
    //                     end = geneEnd + (int) Math.round(distance * 1000);
    //                 } else {  // "downstream".equals(direction) && strand.equals("-1")
    //                     start = geneStart - (int) Math.round(distance * 1000);
    //                     end = includeGene ? geneEnd : geneStart - 1;
    //                 }

    //                 // if the region hangs off the start or end of a chromosome set it to finish
    //                 // at the end of the chromosome
    //                 location.setStart(new Integer(Math.max(start, 1)));
    //                 int e = Math.min(end, chr.getLength().intValue());
    //                 location.setEnd(new Integer(e));

    //                 location.setStrand(strand);
    //                 location.setLocatedOn(chr);
    //                 location.setFeature(region);

    //                 region.setLength(new Integer((location.getEnd().intValue() - location.getStart().intValue()) + 1));

    //                 osw.store(location);
    //                 osw.store(region);
    //             }
    //         }
    //     }
    // }
}
