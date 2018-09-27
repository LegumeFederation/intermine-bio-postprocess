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

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.bio.util.Constants;
import org.intermine.metadata.ConstraintOp;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Exon;
import org.intermine.model.bio.MRNA;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
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
 * Relate exons to genes via mRNAs: exon.mRNA + mRNA.gene gives exon.gene.
 *
 * @author Sam Hokin
 */
public class CreateExonGeneReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateExonGeneReferencesProcess.class);

    /**
     * Create a new instance of CreateExonGeneReferencesProcess
     * @param osw object store writer
-     */
    public CreateExonGeneReferencesProcess(ObjectStoreWriter osw) {
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

        // query exon-mRNA-gene
        Query qExon = new Query();
        qExon.setDistinct(false);
        QueryClass qcExon = new QueryClass(Exon.class);
        qExon.addFrom(qcExon);
        qExon.addToSelect(qcExon);
        qExon.addToOrderBy(qcExon);

        // execute the query
        Results exonResults = osw.getObjectStore().execute(qExon);
        Iterator<?> exonIter = exonResults.iterator();

        osw.beginTransaction();
        while (exonIter.hasNext()) {
            try {
                ResultsRow<?> rr = (ResultsRow<?>) exonIter.next();
                Exon exon = (Exon) rr.get(0);
                String primaryIdentifier = (String) exon.getFieldValue("primaryIdentifier");
                MRNA mRNA = (MRNA) exon.getFieldValue("MRNA");
                Gene gene = (Gene) mRNA.getFieldValue("gene");
                if (gene==null) {
                    LOG.error("Null gene retrieved for exon "+primaryIdentifier);
                } else {
                    Exon tempExon = PostProcessUtil.cloneInterMineObject(exon);
                    Gene tempGene = PostProcessUtil.cloneInterMineObject(gene);
                    tempExon.setFieldValue("gene", tempGene);
                    osw.store(tempExon);
                }
            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            }
        }

        // close transaction
        osw.commitTransaction();

        // close connection
        osw.close();
    }
        
}
