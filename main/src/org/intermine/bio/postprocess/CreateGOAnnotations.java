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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.commons.lang3.StringUtils;

import org.intermine.bio.util.Constants;
import org.intermine.metadata.ConstraintOp;
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

import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GOAnnotation;
import org.intermine.model.bio.GOTerm;

/**
 * Create GOAnnotation records for genes, by parsing the GO term identifiers in their descriptions.
 *
 * @author Sam Hokin
 */
public class CreateGOAnnotations {

    private static final Logger LOG = Logger.getLogger(CreateGOAnnotations.class);
    protected ObjectStoreWriter osw;

    /**
     * Create a new instance of CreateGOAnnotations
     * @param osw object store writer
-     */
    public CreateGOAnnotations(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Main method
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void createGOAnnotations() throws ObjectStoreException, IllegalAccessException {

        // delete existing GOAnnotation objects by first loading them into a collection...
        Set<GOAnnotation> goaSet = new HashSet<GOAnnotation>();
        Query qGOA = new Query();
        QueryClass qcGOA = new QueryClass(GOAnnotation.class);
        qGOA.addToSelect(qcGOA);
        qGOA.addFrom(qcGOA);
        Results goaResults = osw.getObjectStore().execute(qGOA);
        Iterator<?> goaIter = goaResults.iterator();
        while (goaIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) goaIter.next();
            goaSet.add((GOAnnotation)rr.get(0));
        }
        // ...and then deleting them
        LOG.info("Deleting existing GOAnnotation records...");
        osw.beginTransaction();
        for (GOAnnotation goa : goaSet) {
            osw.delete(goa);
        }
        osw.commitTransaction();

        // query all Gene records --> parse out GO terms -> create GOAnnotation entries and references
        
        Query qGene = new Query();
        qGene.setDistinct(true);

        // 0 Gene
        QueryClass qcGene = new QueryClass(Gene.class);
        qGene.addFrom(qcGene);
        qGene.addToSelect(qcGene);
        qGene.addToOrderBy(qcGene);

        osw.beginTransaction();

        // execute the query
        Results geneResults = osw.getObjectStore().execute(qGene);
        Iterator<?> geneIter = geneResults.iterator();
        while (geneIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) geneIter.next();
            Gene gene = (Gene) rr.get(0);
            String description = (String) gene.getFieldValue("description");
            // parse the description for GO identifiers, creating a GOAnnotation each time, and adding it to the gene's collection
            String[] goNumbers = StringUtils.substringsBetween(description, "GO:", " ");
            if (goNumbers!=null) {
                // clone the Gene item for storage
                Gene tempGene = PostProcessUtil.cloneInterMineObject(gene);
                // add the GO terms
                for (int j=0; j<goNumbers.length; j++) {
                    String identifier = "GO:"+goNumbers[j];
                    // query this GO term
                    Query q = new Query();
                    q.setDistinct(true);
                    QueryClass qc = new QueryClass(GOTerm.class);
                    q.addFrom(qc);
                    q.addToSelect(qc);
                    QueryField qfIdentifier = new QueryField(qc, "identifier");
                    ConstraintSet cs = new ConstraintSet(ConstraintOp.AND);
                    SimpleConstraint sc = new SimpleConstraint(qfIdentifier, ConstraintOp.EQUALS, new QueryValue(identifier));
                    cs.addConstraint(sc);
                    q.setConstraint(cs);
                    // execute the query
                    Results results = osw.getObjectStore().execute(q);
                    Iterator<?> iter = results.iterator();
                    if (iter.hasNext()) {
                        ResultsRow<?> row = (ResultsRow<?>) iter.next();
                        GOTerm goTerm = (GOTerm) row.get(0);
                        GOAnnotation goAnnotation = (GOAnnotation) DynamicUtil.createObject(Collections.singleton(GOAnnotation.class));
                        goAnnotation.setFieldValue("ontologyTerm", goTerm);
                        goAnnotation.setFieldValue("subject", gene);
                        osw.store(goAnnotation);
                    }
                }
            }
        }

        osw.commitTransaction();

    }
        
}
