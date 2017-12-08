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

import org.intermine.bio.util.Constants;
import org.intermine.metadata.ConstraintOp;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;
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
 * Relate proteins to genes via transcripts: transcript.protein + transcript.gene = protein.genes, gene.proteins
 *
 * @author Sam Hokin
 */
public class CreateProteinGeneReferences {

    private static final Logger LOG = Logger.getLogger(CreateProteinGeneReferences.class);
    protected ObjectStoreWriter osw;

    /**
     * Create a new instance of CreateProteinGeneReferences
     * @param osw object store writer
-     */
    public CreateProteinGeneReferences(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Main method, create the Protein.gene references
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void createProteinGeneReferences() throws ObjectStoreException, IllegalAccessException {

        // query all transcripts for genes and proteins
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
            ResultsRow<?> rr = (ResultsRow<?>) transcriptIter.next();
            Transcript transcript = (Transcript) rr.get(0);
            String primaryIdentifier = (String) transcript.getFieldValue("primaryIdentifier");
            Gene gene = (Gene) transcript.getFieldValue("gene");
            Protein protein = (Protein) transcript.getFieldValue("protein");
            if (gene!=null && protein!=null) {
                Gene tempGene = PostProcessUtil.cloneInterMineObject(gene);
                Protein tempProtein = PostProcessUtil.cloneInterMineObject(protein);
                // relate the gene and protein and store the relation
                // we'll assume a one-to-one relation here, even though Protein.genes and Gene.proteins are collections
                Set<Protein> proteinCollection = new HashSet<Protein>();
                proteinCollection.add(tempProtein);
                tempGene.setFieldValue("proteins", proteinCollection);
                osw.store(tempGene);
            }
        }

        // close transaction
        osw.commitTransaction();

    }
        
}
