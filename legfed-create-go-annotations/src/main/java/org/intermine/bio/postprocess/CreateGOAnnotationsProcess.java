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

import org.intermine.postprocess.PostProcessor;
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

import org.intermine.model.bio.Annotatable;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.OntologyAnnotation;
import org.intermine.model.bio.OntologyTerm;

/**
 * Create GO OntologyAnnotation records for genes, by parsing the GO term identifiers in their descriptions.
 *
 * @author Sam Hokin
 */
public class CreateGOAnnotationsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateGOAnnotationsProcess.class);

    /**
     * Create a new instance of CreateGOAnnotations
     * @param osw object store writer
-     */
    public CreateGOAnnotationsProcess(ObjectStoreWriter osw) {
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

        // PathQuery query = new PathQuery(model);
        // // Select the output columns:
        // query.addViews("OntologyAnnotation.ontologyTerm.identifier",
        //                "OntologyAnnotation.subject.primaryIdentifier");
        // // Add orderby
        // query.addOrderBy("OntologyAnnotation.ontologyTerm.identifier", OrderDirection.ASC);
        // // Filter the results with the following constraints:
        // query.addConstraint(Constraints.contains("OntologyAnnotation.ontologyTerm.identifier", "GO:"));

        // Find existing GO annotation objects and load the mashed term and gene identifiers into a Set for future non-dupage
        Query qAnnot = new Query();
        qAnnot.setDistinct(true);
        QueryClass qcAnnot = new QueryClass(OntologyAnnotation.class);
        qAnnot.addFrom(qcAnnot);
        qAnnot.addToSelect(qcAnnot);
        // QueryField qfTerm = new QueryField(qcAnnot, "ontologyTerm");
        // ConstraintSet csIdentifier = new ConstraintSet(ConstraintOp.AND);
        // SimpleConstraint scIdentifier = new SimpleConstraint(qfIdentifier, ConstraintOp.CONTAINS, new QueryValue("GO:"));
        // csIdentifier.addConstraint(scIdentifier);
        // qAnnot.setConstraint(csIdentifier);
        Results annotResults = osw.getObjectStore().execute(qAnnot);
        Iterator<?> annotIter = annotResults.iterator();
        Set<String> mashedIdentifiersSet = new HashSet<>();
        while (annotIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) annotIter.next();
            OntologyAnnotation annotation = (OntologyAnnotation)rr.get(0);
            OntologyTerm term = annotation.getOntologyTerm();
            String termIdentifier = term.getIdentifier();
            if (termIdentifier.startsWith("GO:")) {
                Annotatable subject = annotation.getSubject();
                String geneIdentifier = subject.getPrimaryIdentifier();
                String mashedIdentifiers = mashIdentifiers(termIdentifier,geneIdentifier);
                mashedIdentifiersSet.add(mashedIdentifiers);
            }
        }

        // query all Gene records, loading Genes into a Set
        Query qGene = new Query();
        qGene.setDistinct(true);
        QueryClass qcGene = new QueryClass(Gene.class);
        qGene.addFrom(qcGene);
        qGene.addToSelect(qcGene);
        qGene.addToOrderBy(qcGene);
        Results geneResults = osw.getObjectStore().execute(qGene);
        Iterator<?> geneIter = geneResults.iterator();
        Set<Gene> geneSet = new HashSet<>();
        while (geneIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) geneIter.next();
            geneSet.add((Gene)rr.get(0));
        }
        LOG.info("Retrieved "+geneSet.size()+" Gene objects for GO annotation.");

        // now plow through the genes, creating GO annotation records if they don't already exist
        int count = 0;
        for (Gene gene : geneSet) {
            try {
                String geneIdentifier = (String) gene.getFieldValue("primaryIdentifier");
	        String description = (String) gene.getFieldValue("description");
                // parse the description for GO identifiers, assuming comma-space format
                String[] goNumbers = StringUtils.substringsBetween(description, "GO:", " ");
                if (goNumbers!=null) {
                    // create and store the GO annotations
                    osw.beginTransaction();
                    for (int i=0; i<goNumbers.length; i++) {
                        String termIdentifier = "GO:"+goNumbers[i];
                        String mashedIdentifiers = mashIdentifiers(termIdentifier, geneIdentifier);
                        if (!mashedIdentifiersSet.contains(mashedIdentifiers)) {
                            // query this ontology term
                            Query q = new Query();
                            q.setDistinct(true);
                            QueryClass qc = new QueryClass(OntologyTerm.class);
                            q.addFrom(qc);
                            q.addToSelect(qc);
                            QueryField qf = new QueryField(qc, "identifier");
                            ConstraintSet cs = new ConstraintSet(ConstraintOp.AND);
                            SimpleConstraint sc = new SimpleConstraint(qf, ConstraintOp.EQUALS, new QueryValue(termIdentifier));
                            cs.addConstraint(sc);
                            q.setConstraint(cs);
                            // execute the query
                            Results results = osw.getObjectStore().execute(q);
                            Iterator<?> iter = results.iterator();
                            if (iter.hasNext()) {
                                ResultsRow<?> row = (ResultsRow<?>) iter.next();
                                OntologyTerm term = (OntologyTerm) row.get(0);
                                OntologyAnnotation goAnnotation = (OntologyAnnotation) DynamicUtil.createObject(Collections.singleton(OntologyAnnotation.class));
                                goAnnotation.setFieldValue("ontologyTerm", term);
                                goAnnotation.setFieldValue("subject", gene);
                                osw.store(goAnnotation);
                                count++;
                            } else {
                                LOG.error("GO term not found for ["+termIdentifier+"]");
                            }
                        }
                    }
                    osw.commitTransaction();
                }
            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            }
        }
        LOG.info("Stored "+count+" additional GO annotations.");
    }

    String mashIdentifiers(String termIdentifier, String geneIdentifier) {
        return termIdentifier+"_"+geneIdentifier;
    }
}
