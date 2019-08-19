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

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.bio.util.Constants;
import org.intermine.metadata.ConstraintOp;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.MRNA;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
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

/**
 * Relate mRNAs to genes by simply truncating the .n to find the gene identifier
 *
 * @author Sam Hokin
 */
public class CreateMRNAGeneReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateMRNAGeneReferencesProcess.class);

    /**
     * Create a new instance of CreateMRNAGeneReferencesProcess
     * @param osw object store writer
-     */
    public CreateMRNAGeneReferencesProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     *
     * Main method, create the MRNA.gene references
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void postProcess() throws ObjectStoreException {

        // query all mRNAs for genes and proteins
        Query qMRNA = new Query();
        qMRNA.setDistinct(false);
        QueryClass qcMRNA = new QueryClass(MRNA.class);
        qMRNA.addFrom(qcMRNA);
        qMRNA.addToSelect(qcMRNA);
        qMRNA.addToOrderBy(qcMRNA);

        // execute the query
        Results mRNAResults = osw.getObjectStore().execute(qMRNA);
        Iterator<?> mRNAIter = mRNAResults.iterator();

        // begin transaction
        osw.beginTransaction();

        while (mRNAIter.hasNext()) {
            try {
                ResultsRow<?> rr = (ResultsRow<?>) mRNAIter.next();
                MRNA mRNA = (MRNA) rr.get(0);
                String primaryIdentifier = (String) mRNA.getFieldValue("primaryIdentifier");
                String[] parts = primaryIdentifier.split("\\.");
                int n = Integer.parseInt(parts[parts.length-1]);
                String geneIdentifier = parts[0];
                for (int i=1; i<parts.length-1; i++) {
                    geneIdentifier += "."+parts[i];
                }
                // query this particular gene
                Query qGene = new Query();
                qGene.setDistinct(false);
                QueryClass qcGene = new QueryClass(Gene.class);
                qGene.addFrom(qcGene);
                qGene.addToSelect(qcGene);
                QueryField qfIdentifier = new QueryField(qcGene, "primaryIdentifier");
                ConstraintSet cs = new ConstraintSet(ConstraintOp.AND);
                SimpleConstraint sc = new SimpleConstraint(qfIdentifier, ConstraintOp.EQUALS, new QueryValue(geneIdentifier));
                cs.addConstraint(sc);
                qGene.setConstraint(cs);
                // execute the query
                Results geneResults = osw.getObjectStore().execute(qGene);
                Iterator<?> geneIter = geneResults.iterator();
                if (geneIter.hasNext()) {
                    ResultsRow<?> row = (ResultsRow<?>) geneIter.next();
                    Gene gene = (Gene) row.get(0);
                    MRNA tempMRNA = PostProcessUtil.cloneInterMineObject(mRNA);
                    tempMRNA.setFieldValue("gene", gene);
                    osw.store(tempMRNA);
                } else {
                    LOG.error("Gene not found for ["+geneIdentifier+"]");
                }
            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            } catch (NumberFormatException ex) {
                // do nothing, this mRNA name is not gene-friendly
            }
        }

        // close transaction
        osw.commitTransaction();

    }
        
}
