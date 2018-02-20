package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2002-2016 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.intermine.bio.util.BioQueries;
import org.intermine.metadata.ClassDescriptor;
import org.intermine.metadata.MetaDataException;
import org.intermine.model.bio.Supercontig;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GeneFlankingRegion;
import org.intermine.model.bio.Location;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.util.DynamicUtil;

/**
 * Create features to represent flanking regions of configurable distance either side of gene
 * features.  These will be used in overlap queries.
 *
 * @author rns
 * @author Sam Hokin
 *
 */
public class CreateSupercontigFlankingRegions {

    private static final Logger LOG = Logger.getLogger(CreateSupercontigFlankingRegions.class);

    private ObjectStoreWriter osw = null;
    private ObjectStore os;
    private Map<Integer, Supercontig> chrs = new HashMap<Integer, Supercontig>();

    // the sizes in kb of flanking regions to create
    private static double[] distances = new double[] {0.5};

    // the values strings for up/downstream from a gene
    private static String[] directions = new String[] {"upstream", "downstream"};

    // whether to include the gene sequence in the "flanking" region
    private static boolean[] includeGenes = new boolean[] {false};

    /**
     * Create a new CreateSupercontigFlankingRegions object that will operate on the given
     * ObjectStoreWriter.
     *
     * @param osw the ObjectStoreWriter to use when creating/changing objects
     */
    public CreateSupercontigFlankingRegions(ObjectStoreWriter osw) {
        this.osw = osw;
        this.os = osw.getObjectStore();
    }

    /**
     * Iterate over genes in database and create flanking regions.
     * @throws ObjectStoreException if something goes wrong
     */
    public void createFlankingFeatures() throws ObjectStoreException {

        Results results = BioQueries.findLocationAndObjects(os, Supercontig.class, Gene.class, false, false, false, 1000);

        Iterator<?> resIter = results.iterator();

        int count = 0;

        osw.beginTransaction();
        while (resIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) resIter.next();
            Integer chrId = (Integer) rr.get(0);
            Gene gene = (Gene) rr.get(1);
            Location loc = (Location) rr.get(2);
            createAndStoreFlankingRegion(getSupercontig(chrId), loc, gene);
            if ((count % 1000) == 0) {
                LOG.info("Created flanking regions for " + count + " genes.");
            }
            count++;
        }
        osw.commitTransaction();
    }


    private void createAndStoreFlankingRegion(Supercontig chr, Location geneLoc, Gene gene) throws ObjectStoreException {
	
        // This code can't cope with supercontigs that don't have a length
        if (chr.getLength() == null) {
            LOG.warn("Attempted to create GeneFlankingRegions on a supercontig without a length: " + chr.getPrimaryIdentifier());
            return;
        }

        for (double distance : distances) {
            for (String direction : directions) {
                for (boolean includeGene : includeGenes) {
                    String strand = geneLoc.getStrand();

                    // TODO what do we do if strand not set?
                    int geneStart = geneLoc.getStart().intValue();
                    int geneEnd = geneLoc.getEnd().intValue();
                    int chrLength = chr.getLength().intValue();

                    // gene touches a supercontig end so there isn't a flanking region
                    if ((geneStart <= 1) || (geneEnd >= chrLength)) {
                        continue;
                    }

                    GeneFlankingRegion region = (GeneFlankingRegion) DynamicUtil.createObject(Collections.singleton(GeneFlankingRegion.class));
                    Location location = (Location) DynamicUtil.createObject(Collections.singleton(Location.class));

                    region.setDistance("" + distance + "kb");
                    region.setDirection(direction);
                    try {
                        PostProcessUtil.checkFieldExists(os.getModel(), "GeneFlankingRegion", "includeGene", "Not setting");
                        region.setFieldValue("includeGene", Boolean.valueOf(includeGene));
                    } catch (MetaDataException e) {
                        // GeneFlankingRegion.includeGene not in model so do nothing
                    }
                    region.setGene(gene);
                    region.setSupercontig(chr);
                    region.setSupercontigLocation(location);
                    region.setOrganism(gene.getOrganism());
                    region.setPrimaryIdentifier(gene.getPrimaryIdentifier() + " " + distance + "kb " + direction);

                    // this should be some clever algorithm
                    int start, end;

                    if ("upstream".equals(direction) && "1".equals(strand)) {
                        start = geneStart - (int) Math.round(distance * 1000);
                        end = includeGene ? geneEnd : geneStart - 1;
                    } else if ("upstream".equals(direction) && "-1".equals(strand)) {
                        start = includeGene ? geneStart : geneEnd + 1;
                        end = geneEnd + (int) Math.round(distance * 1000);
                    } else if ("downstream".equals(direction) && "1".equals(strand)) {
                        start = includeGene ? geneStart : geneEnd + 1;
                        end = geneEnd + (int) Math.round(distance * 1000);
                    } else {  // "downstream".equals(direction) && strand.equals("-1")
                        start = geneStart - (int) Math.round(distance * 1000);
                        end = includeGene ? geneEnd : geneStart - 1;
                    }

                    // if the region hangs off the start or end of a supercontig set it to finish
                    // at the end of the supercontig
                    location.setStart(new Integer(Math.max(start, 1)));
                    int e = Math.min(end, chr.getLength().intValue());
                    location.setEnd(new Integer(e));

                    location.setStrand(strand);
                    location.setLocatedOn(chr);
                    location.setFeature(region);

                    region.setLength(new Integer((location.getEnd().intValue() - location.getStart().intValue()) + 1));

                    osw.store(location);
                    osw.store(region);
                }
            }
        }
    }

    private Supercontig getSupercontig(Integer chrId) throws ObjectStoreException {
        Supercontig chr = chrs.get(chrId);
        if (chr == null) {
            chr = (Supercontig) os.getObjectById(chrId, Supercontig.class);
            chrs.put(chrId, chr);
        }
        return chr;
    }
}
