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

        // query Transcripts and Proteins with the same primaryIdentifier
        Query query = new Query();
        query.setDistinct(false);
        
        QueryClass qcTranscript = new QueryClass(Transcript.class);
        query.addFrom(qcTranscript);
        query.addToSelect(qcTranscript);

        QueryClass qcProtein = new QueryClass(Protein.class);
        query.addFrom(qcProtein);
        query.addToSelect(qcProtein);

        query.setConstraint(new SimpleConstraint(new QueryField(qcProtein,"primaryIdentifier"), ConstraintOp.EQUALS, new QueryField(qcTranscript,"primaryIdentifier")));
        
        // execute the query
        Results results = osw.getObjectStore().execute(query);
        Iterator<?> iter = results.iterator();

        // begin transaction
        osw.beginTransaction();

        while (iter.hasNext()) {
            try {
                ResultsRow<?> rr = (ResultsRow<?>) iter.next();
                Transcript transcript = (Transcript) rr.get(0);
                Protein protein = (Protein) rr.get(1);
                Gene gene = (Gene) transcript.getFieldValue("gene");
                if (gene!=null) {
                    // clone the Protein and add the Gene to the Protein.genes collection
                    Protein tempProtein = PostProcessUtil.cloneInterMineObject(protein);
                    Set<Gene> geneCollection = new HashSet<>();
                    geneCollection.add(gene);
                    tempProtein.setFieldValue("genes", geneCollection);
                    osw.store(tempProtein);
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
