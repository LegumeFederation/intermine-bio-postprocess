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
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.apache.log4j.Logger;

/**
 * Relate proteins to genes via transcripts: transcript.gene gives protein.genes where transcript.primaryIdentifier=protein.primaryIdentifier.
 *
 * @author Sam Hokin
 */
public class CreateProteinGeneReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateProteinGeneReferencesProcess.class);

    /**
     * Create a new instance of CreateProteinGeneReferencesProcess
     * @param osw object store writer
-     */
    public CreateProteinGeneReferencesProcess(ObjectStoreWriter osw) {
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

        // query all transcripts
        Query qTranscript = new Query();
        qTranscript.setDistinct(false);
        QueryClass qcTranscript = new QueryClass(Transcript.class);
        qTranscript.addFrom(qcTranscript);
        qTranscript.addToSelect(qcTranscript);
        qTranscript.addToOrderBy(qcTranscript);

        // execute the query
        Results transcriptResults = osw.getObjectStore().execute(qTranscript);
        Iterator<?> transcriptIter = transcriptResults.iterator();

        // begin transaction
        osw.beginTransaction();

        while (transcriptIter.hasNext()) {
            try {
                ResultsRow<?> rr = (ResultsRow<?>) transcriptIter.next();
                Transcript transcript = (Transcript) rr.get(0);
                String primaryIdentifier = (String) transcript.getFieldValue("primaryIdentifier");
                Gene gene = (Gene) transcript.getFieldValue("gene");
                if (gene!=null) {
                    // query the Protein
                    Query q = new Query();
                    q.setDistinct(true);
                    QueryClass qc = new QueryClass(Protein.class);
                    q.addFrom(qc);
                    q.addToSelect(qc);
                    QueryField qf = new QueryField(qc, "primaryIdentifier");
                    SimpleConstraint sc = new SimpleConstraint(qf, ConstraintOp.EQUALS, new QueryValue(primaryIdentifier));
                    q.setConstraint(sc);
                    // execute the query
                    Results results = osw.getObjectStore().execute(q);
                    Iterator<?> iter = results.iterator();
                    if (iter.hasNext()) {
                        ResultsRow<?> row = (ResultsRow<?>) iter.next();
                        Protein protein = (Protein) row.get(0);
                        // clone the Protein and add the Gene to the Protein.genes collection
                        Protein tempProtein = PostProcessUtil.cloneInterMineObject(protein);
                        Set<Gene> geneCollection = new HashSet<>();
                        geneCollection.add(gene);
                        tempProtein.setFieldValue("genes", geneCollection);
                        osw.store(tempProtein);
                    }
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
