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

import java.text.DecimalFormat;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.BioEntity;
import org.intermine.model.bio.OntologyAnnotation;
import org.intermine.model.bio.OntologyTerm;

import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.OrderDescending;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * For given ontology annotations, create additional annotations with those ontology terms' parents.
 * This allows one to only specify the deepest ontology term for, say, a QTL, but be able to query the mine for higher-level terms.
 *
 * @author Sam Hokin
 */
public class CreateOntologyParentAnnotations {

    private static final Logger LOG = Logger.getLogger(CreateOntologyParentAnnotations.class);
    protected ObjectStoreWriter osw;

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public CreateOntologyParentAnnotations(ObjectStoreWriter osw) {
        this.osw = osw;
    }

    /**
     * Run the analysis.
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void createOntologyParentAnnotations() throws ObjectStoreException, IllegalAccessException {
        
        LOG.info("Querying OntologyAnnotation records and associated terms and parents...");

        // <class name="OntologyAnnotation" is-interface="true">
	//   <reference name="ontologyTerm" referenced-type="OntologyTerm" reverse-reference="ontologyAnnotations"/>
	//   <reference name="subject" referenced-type="BioEntity" reverse-reference="ontologyAnnotations"/>
        // </class>

        // <class name="OntologyTerm" is-interface="true">
	//   <attribute name="identifier" type="java.lang.String"/>
	//   <attribute name="name" type="java.lang.String"/>
	//   <collection name="ontologyAnnotations" referenced-type="OntologyAnnotation" reverse-reference="ontologyTerm"/>
	//   <collection name="parents" referenced-type="OntologyTerm"/>
        // </class>
        
        Query qOntologyAnnotation = new Query();
        qOntologyAnnotation.setDistinct(true);
        ConstraintSet csOntologyAnnotation = new ConstraintSet(ConstraintOp.AND);

        // 0 OntologyAnnotation
        QueryClass qcOntologyAnnotation = new QueryClass(OntologyAnnotation.class);
        qOntologyAnnotation.addToSelect(qcOntologyAnnotation);
        qOntologyAnnotation.addFrom(qcOntologyAnnotation);

        // 1 OntologyAnnotation.ontologyTerm
        QueryClass qcOntologyTerm = new QueryClass(OntologyTerm.class);
        qOntologyAnnotation.addToSelect(qcOntologyTerm);
        qOntologyAnnotation.addFrom(qcOntologyTerm);
        QueryObjectReference ontologyTerm = new QueryObjectReference(qcOntologyAnnotation, "ontologyTerm");
        csOntologyAnnotation.addConstraint(new ContainsConstraint(ontologyTerm, ConstraintOp.CONTAINS, qcOntologyTerm));

        // LIMIT TO TO: terms for now
        csOntologyAnnotation.addConstraint(new SimpleConstraint(new QueryField(qcOntologyTerm,"identifier"), ConstraintOp.MATCHES, new QueryValue("TO:%")));

        // 2 OntologyAnnotation.subject
        QueryClass qcSubject = new QueryClass(BioEntity.class);
        qOntologyAnnotation.addToSelect(qcSubject);
        qOntologyAnnotation.addFrom(qcSubject);
        QueryObjectReference subj = new QueryObjectReference(qcOntologyAnnotation, "subject");
        csOntologyAnnotation.addConstraint(new ContainsConstraint(subj, ConstraintOp.CONTAINS, qcSubject));

        // 3 OntologyAnnotation.ontologyTerm.parents
        QueryClass qcParents = new QueryClass(OntologyTerm.class);
        qOntologyAnnotation.addToSelect(qcParents);
        qOntologyAnnotation.addFrom(qcParents);
        QueryCollectionReference parents = new QueryCollectionReference(qcOntologyTerm, "parents");
        csOntologyAnnotation.addConstraint(new ContainsConstraint(parents, ConstraintOp.CONTAINS, qcParents));

        // set the constraints
        qOntologyAnnotation.setConstraint(csOntologyAnnotation);

        // begin transaction
        osw.beginTransaction();

        // execute the query
        Results otResults = osw.getObjectStore().execute(qOntologyAnnotation);
        Iterator<?> otIter = otResults.iterator();
        while (otIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) otIter.next();
            // objects
            OntologyAnnotation annotation = (OntologyAnnotation) rr.get(0);
            OntologyTerm term = (OntologyTerm) rr.get(1);
            BioEntity subject = (BioEntity) rr.get(2);
            OntologyTerm parent = (OntologyTerm) rr.get(3);
            // field values
            String termIdentifier = (String) term.getFieldValue("identifier");
            String termName = (String) term.getFieldValue("name");
            String subjectId = (String) subject.getFieldValue("primaryIdentifier");
            String parentIdentifier = (String) parent.getFieldValue("identifier");
            String parentName = (String) parent.getFieldValue("name");
            // create and store a new ontology annotation with this subject and a new term from the parent
            OntologyAnnotation newAnnotation = (OntologyAnnotation) DynamicUtil.createObject(Collections.singleton(OntologyAnnotation.class));
            newAnnotation.setSubject(subject);
            newAnnotation.setOntologyTerm(parent);
            osw.store(newAnnotation);
            // DEBUG
            LOG.info(subjectId+"\t"+termIdentifier+":"+termName);
            LOG.info("+\t\t"+parentIdentifier+":"+parentName);
        }

        // commit and close transaction
        osw.commitTransaction();

    }

}
