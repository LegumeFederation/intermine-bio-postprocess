package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2016 Flymine, Legume Federation
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import org.apache.log4j.Logger;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.model.bio.ProteinDomain;

import org.ncgr.interpro.DbXref;
import org.ncgr.interpro.DBInfo;
import org.ncgr.interpro.Interpro;
import org.ncgr.interpro.InterproReader;

import com.thoughtworks.xstream.XStreamException;

/**
 * Download the Interpro XML file from their FTP site and populate ProteinDomain fields as well as associate Proteins with ProteinDomains via ProteinHmmMatch.
 *
 * @author Sam Hokin
 */
public class PopulateInterproDataProcess extends PostProcessor {

    public static final String FTPSERVER = "ftp.ebi.ac.uk";
    public static final String FTPFILENAME = "/pub/databases/interpro/interpro.xml.gz";
    public static final String GZFILENAME = "/tmp/interpro.xml.gz";
    public static final String XMLFILENAME = "/tmp/interpro.xml";

    private static final Logger LOG = Logger.getLogger(PopulateInterproDataProcess.class);

    // main maps from InterproReader
    Map<String,DBInfo> dbInfoMap;
    Map<String,Interpro> interproMap;
    Set<String> delRefSet;

    // special-purpose maps used here
    Map<String,Interpro> gene3dMap; // maps CATHGENE3D ids to Interpro objects
    Map<String,Interpro> pfamMap;   // maps PFAM ids to Interpro objects
    Map<String,String> pfamNameMap; // maps PFAM ids to PFAM names (e.g. PF01582 to TIR)
    Map<String,Interpro> pirsfMap;   // maps PIRSF ids to Interpro objects
    Map<String,String> pirsfNameMap; // maps PIRSF ids to PIRSF names 
    Map<String,Interpro> smartMap;   // maps SMART ids to Interpro objects
    Map<String,String> smartNameMap; // maps SMART ids to SMART names 
    Map<String,Interpro> tigrMap;   // maps TIGRFAMs ids to Interpro objects
    Map<String,String> tigrNameMap; // maps TIGRFAMs ids to TIGR names 

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore.
     * Downloads the Interpro XML file and creates the maps used in populateProteinDomains.
     *
     * @param osw object store writer
     */
    public PopulateInterproDataProcess(ObjectStoreWriter osw) {

        super(osw);
        
        // download the xml.gz file
        LOG.info("Downloading "+FTPSERVER+":"+FTPFILENAME+"...");
        download(FTPSERVER, FTPFILENAME, GZFILENAME);
        LOG.info("Done.");

        // gunzip it
        LOG.info("Unzipping "+GZFILENAME+" to "+XMLFILENAME+"...");
        gunzip(GZFILENAME, XMLFILENAME);
        LOG.info("Done.");

        File xmlFile = new File(XMLFILENAME);
        if (!xmlFile.canRead()) {
            throw new RuntimeException("Can't read xmlFile: "+xmlFile.getName());
        }

        // read the XML file
        LOG.info("Reading "+xmlFile.getName()+"; length="+xmlFile.length()+"...");
        InterproReader reader = new InterproReader();
        reader.read(xmlFile);
        LOG.info("Done.");
        
        // load Interpro data maps
        dbInfoMap = reader.getDBInfoMap();
        interproMap = reader.getInterproMap();
        delRefSet = reader.getDelRefSet();

        // build the special-purpose maps
        LOG.info("Building CATHGENE3D, PFAM, PIRSF, TIGRFAMs and SMART maps...");
        gene3dMap = new HashMap<String,Interpro>();  // CATHGENE3D does not have short names
        pfamMap = new HashMap<String,Interpro>();
        pfamNameMap = new HashMap<String,String>();
        tigrMap = new HashMap<String,Interpro>();
        tigrNameMap = new HashMap<String,String>();
        smartMap = new HashMap<String,Interpro>();
        smartNameMap = new HashMap<String,String>();
        pirsfMap = new HashMap<String,Interpro>();
        pirsfNameMap = new HashMap<String,String>();
        for (Interpro interpro : interproMap.values()) {
            List<DbXref> members = interpro.memberList.getEntries();
            for (DbXref member : members) {
                if (member.db.equals("CATHGENE3D")) {
                    gene3dMap.put(member.dbkey, interpro);
                    // CATHGENE3D does not have distinct short names
                } else if (member.db.equals("PFAM")) {
                    pfamMap.put(member.dbkey, interpro);
                    pfamNameMap.put(member.dbkey, member.name);
                } else if (member.db.equals("PIRSF")) {
                    pirsfMap.put(member.dbkey, interpro);
                    pirsfNameMap.put(member.dbkey, member.name);
                } else if (member.db.equals("SMART")) {
                    smartMap.put(member.dbkey, interpro);
                    smartNameMap.put(member.dbkey, member.name);
                } else if (member.db.equals("TIGRFAMs")) {
                    tigrMap.put(member.dbkey, interpro);
                    tigrNameMap.put(member.dbkey, member.name);
                }
            }
        }
        LOG.info("Done.");
        
    }

    /**
     * {@inheritDoc}
     *
     * Main method
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void postProcess() throws ObjectStoreException {

        // query all protein domains
        Query qProteinDomain = new Query();
        qProteinDomain.setDistinct(false);
        QueryClass qcProteinDomain = new QueryClass(ProteinDomain.class);
        qProteinDomain.addFrom(qcProteinDomain);
        qProteinDomain.addToSelect(qcProteinDomain);
        qProteinDomain.addToOrderBy(qcProteinDomain);

        // execute the query
        Results proteinDomainResults = osw.getObjectStore().execute(qProteinDomain);
        Iterator<?> proteinDomainIter = proteinDomainResults.iterator();

        // begin transaction
        osw.beginTransaction();

        LOG.info("Populating protein domains...");
        while (proteinDomainIter.hasNext()) {
            try {
            
                ResultsRow<?> rr = (ResultsRow<?>) proteinDomainIter.next();
                ProteinDomain proteinDomain = (ProteinDomain) rr.get(0);
                String id = (String) proteinDomain.getFieldValue("primaryIdentifier");
                
                Interpro interpro = null;
                String name = null; // all but G3D have PD database names
                
                if (id.startsWith("G3D") && gene3dMap.containsKey(id)) {
                    interpro = gene3dMap.get(id);
                } else if (id.startsWith("PF") && pfamMap.containsKey(id)) {
                    interpro = pfamMap.get(id);
                    name = pfamNameMap.get(id);
                } else if (id.startsWith("PIRSF") && pirsfMap.containsKey(id)) {
                    interpro = pirsfMap.get(id);
                    name = pirsfNameMap.get(id);
                } else if (id.startsWith("TIGR") && tigrMap.containsKey(id)) {
                    interpro = tigrMap.get(id);
                    name = tigrNameMap.get(id);
                } else if (id.startsWith("SM") && smartMap.containsKey(id)) {
                    interpro = smartMap.get(id);
                    name = smartNameMap.get(id);
                }
                
                if (interpro!=null) {
                    ProteinDomain tempProteinDomain = PostProcessUtil.cloneInterMineObject(proteinDomain);
                    if (name!=null) tempProteinDomain.setFieldValue("name", name);
                    tempProteinDomain.setFieldValue("interproId", interpro.id);
                    tempProteinDomain.setFieldValue("interproName", interpro.name);
                    tempProteinDomain.setFieldValue("interproShortName", interpro.shortName);
                    osw.store(tempProteinDomain);
                }

            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            }

        }
        LOG.info("Done.");

        // close transaction
        osw.commitTransaction();

    }


 
    /**
     * Download a binary file from an FTP site using Apache Commons Net API.
     */
    void download(String ftpServer, String ftpFilename, String downloadFilename) {
        
        String server = ftpServer;
        int port = 21;
        String user = "anonymous";
        String pass = "";
 
        FTPClient ftpClient = new FTPClient();
        try {
 
            ftpClient.connect(server, port);
            ftpClient.login(user, pass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
 
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFilename));
            boolean success = ftpClient.retrieveFile(ftpFilename, outputStream);
            outputStream.close();
 
            if (success) {
                return;
            }
  
        } catch (IOException ex) {
            throw new RuntimeException("Cannot download "+ftpFilename+":"+ex.getMessage());
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                LOG.info(ex.toString());
            }
        }
    }


    /**
     * decompress a gzipped file
     */
    void gunzip(String gzippedFile, String unzippedFile){
        byte[] buffer = new byte[1024];
        try {
            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(gzippedFile));
            FileOutputStream out = new FileOutputStream(unzippedFile);
            int len;
            while ((len=gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            gzis.close();
            out.close();
        } catch(IOException ex) {
            throw new RuntimeException("Could not unzip "+gzippedFile+": "+ex.getMessage());
        }
    }

}
