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

import org.intermine.model.bio.Protein;
import org.intermine.model.bio.ProteinHmmMatch;
import org.intermine.model.bio.ProteinDomain;

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
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.apache.log4j.Logger;

/**
 * Relate ProteinDomain to Protein via ProteinHmmMatch records, which reference both.
 *
 * @author Sam Hokin
 */
public class CreateProteinDomainReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateProteinDomainReferencesProcess.class);

    /**
     * Create a new instance of CreateProteinDomainReferencesProcess
     * @param osw object store writer
-     */
    public CreateProteinDomainReferencesProcess(ObjectStoreWriter osw) {
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
        try {

            // ------------------------------------------------------------------
            // query all ProteinDomain records --> proteinHmmMatches --> Proteins
            // ------------------------------------------------------------------
        
            Query qProteinDomain = new Query();
            qProteinDomain.setDistinct(true);
            ConstraintSet csProteinDomain = new ConstraintSet(ConstraintOp.AND);

            // 0 ProteinDomain
            QueryClass qcProteinDomain = new QueryClass(ProteinDomain.class);
            qProteinDomain.addFrom(qcProteinDomain);
            qProteinDomain.addToSelect(qcProteinDomain);
            qProteinDomain.addToOrderBy(qcProteinDomain);

            // 1 ProteinDomain.proteinHmmMatches
            QueryClass qcProteinHmmMatch = new QueryClass(ProteinHmmMatch.class);
            qProteinDomain.addFrom(qcProteinHmmMatch);
            qProteinDomain.addToSelect(qcProteinHmmMatch);
            qProteinDomain.addToOrderBy(qcProteinHmmMatch);
            QueryCollectionReference pdProteinHmmMatches = new QueryCollectionReference(qcProteinDomain, "proteinHmmMatches");
            csProteinDomain.addConstraint(new ContainsConstraint(pdProteinHmmMatches, ConstraintOp.CONTAINS, qcProteinHmmMatch));

            // set the overall constraint
            qProteinDomain.setConstraint(csProteinDomain);

            // execute the query
            Results proteinDomainResults = osw.getObjectStore().execute(qProteinDomain);
            Iterator<?> proteinDomainIter = proteinDomainResults.iterator();

            // begin transaction
            osw.beginTransaction();

            // probably a better way to do this...
            int lastID = 0;
            ProteinDomain tempProteinDomain = null;
            Set<Protein> proteinCollection = new HashSet<Protein>();
        
            while (proteinDomainIter.hasNext()) {
                ResultsRow<?> rr = (ResultsRow<?>) proteinDomainIter.next();
                ProteinDomain proteinDomain = (ProteinDomain) rr.get(0);
                ProteinHmmMatch proteinHmmMatch = (ProteinHmmMatch) rr.get(1);
                Protein protein = (Protein) proteinHmmMatch.getFieldValue("protein");
                if (protein!=null) {
                    int ID = proteinDomain.getId().intValue();
                    Protein tempProtein = PostProcessUtil.cloneInterMineObject(protein);
                    if (ID==lastID) {
                        // add to existing set
                        proteinCollection.add(tempProtein);
                    } else {
                        // store the previous collection with the previous protein domain
                        if (proteinCollection.size()>0) {
                            tempProteinDomain.setFieldValue("proteins", proteinCollection);
                            osw.store(tempProteinDomain);
                        }
                        // start a new collection for this protein domain
                        lastID = ID;
                        tempProteinDomain = PostProcessUtil.cloneInterMineObject(proteinDomain);
                        proteinCollection = new HashSet<Protein>();
                        proteinCollection.add(tempProtein);
                    }
                }
            }
            // do the last one
            if (proteinCollection.size()>0) {
                tempProteinDomain.setFieldValue("proteins", proteinCollection);
                osw.store(tempProteinDomain);
            }

        } catch (IllegalAccessException ex) {
            throw new ObjectStoreException(ex);
        }

        // close transaction
        osw.commitTransaction();
        
    }
        
}
