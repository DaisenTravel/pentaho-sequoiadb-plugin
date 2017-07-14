/**
 *      Copyright (C) 2012 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BSONObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;

public class SequoiaDBOutput extends BaseStep implements StepInterface {

   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   private SequoiaDBOutputMeta m_meta ;

   private SequoiaDBOutputData m_data ;

   private DBCollection m_cl ;

   protected int m_bulkInsertSize ;

   private List<BSONObject> m_buffer ;
   
   private boolean m_truncate = false ;
   
   private boolean m_update = false ;
   
   private boolean m_upsert = false ;

   public SequoiaDBOutput(StepMeta stepMeta,
         StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
         Trans trans) {
      super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
   }

   @Override
   public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
      if(super.init(stepMetaInterface, stepDataInterface)){
         m_meta = (SequoiaDBOutputMeta)stepMetaInterface;
         m_data = (SequoiaDBOutputData)stepDataInterface;
         
         String connString = environmentSubstitute(m_meta.getHostname())
               + ":" + environmentSubstitute(m_meta.getPort());
         Sequoiadb sdb = null;
         sdb = new Sequoiadb(connString, m_meta.getUserName(), m_meta.getPwd());
         CollectionSpace cs ;
         if(!sdb.isCollectionSpaceExist(m_meta.getCSName())){
            cs = sdb.createCollectionSpace( m_meta.getCSName() );
         }
         else{
            cs = sdb.getCollectionSpace( m_meta.getCSName() );
         }

         if(!cs.isCollectionExist( m_meta.getCLName() )){
            m_cl = cs.createCollection( m_meta.getCLName() );
         }
         else{
            m_cl = cs.getCollection( m_meta.getCLName() );
         }
         
         m_bulkInsertSize = m_meta.getBulkInsertSize();
         m_truncate = m_meta.getTruncate();
         m_update = m_meta.getUpdate();
         m_upsert = m_meta.getUpsert();

         return true;
      }
      return false;
   }

   @Override
   public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
      super.dispose(smi, sdi);
   }

   @Override
   public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException{
      // safely cast the step settings (meta) and runtime info (data) to specific implementations 
      // SequoiaDBOutputMeta meta = (SequoiaDBOutputMeta) smi;
      // SequoiaDBOutputData data = (SequoiaDBOutputData) sdi;

      // get incoming row, getRow() potentially blocks waiting for more rows, returns null if no more rows expected
      Object[] r = getRow(); 
      
      // if no more rows are expected, indicate step is finished and processRow() should not be called again
      if (r == null){
         flushToDB();
         setOutputDone();
         return false;
      }

      // the "first" flag is inherited from the base step implementation
      // it is used to guard some processing tasks, like figuring out field indexes
      // in the row structure that only need to be done once
      if (first) {
         first = false;
         
         if(m_truncate) {
            m_cl.truncate();
         }

         // clone the input row structure and place it in our data object
         m_buffer = new ArrayList< BSONObject >( m_bulkInsertSize ) ;

         // output the same as the input
         RowMetaInterface rmi = getInputRowMeta() ;
         m_data.setOutputRowMeta( rmi ) ;

         // check if match the input fields
         List<SequoiaDBOutputFieldInfo> selectedFields = m_meta.getSelectedFields();
         if ( null != selectedFields ) {
            checkFields( rmi, selectedFields ) ;
         }
      }

      // log progress if it is time to to so
      if (checkFeedback(getLinesRead())) {
         logBasic("Linenr " + getLinesRead()); // Some basic logging
      }
      List<SequoiaDBOutputFieldInfo> l = m_meta.getSelectedFields() ;
      if ( null != l && 0 != l.size() ) {
         if(!m_update) {
            BSONObject recObj = m_data.getInserter( r, m_meta.getOutputRecordInfo() );
            if ( recObj != null ) {
               m_buffer.add( recObj ) ;
            }
            
            if ( m_buffer.size() >= m_bulkInsertSize ){
               flushToDB() ;
            }
         }
         else {
            BSONObject ruler = m_data.getUpdater( r, m_meta.getOutputRecordInfo() );
            BSONObject cond = m_data.getUpdateCond( r, m_meta.getOutputRecordInfo() ) ;
            if( m_upsert ) {
               m_cl.upsert( cond, ruler, null );
            }
            else {
               m_cl.update( cond, ruler, null );
            }
         }
      }   

      // indicate that processRow() should be called again
      return true;
   }
   
   void flushToDB() {
      if ( m_buffer != null && m_buffer.size() > 0 ){
         m_cl.bulkInsert( m_buffer, 0  );
         m_buffer.clear();
      }
   }
   
   void checkFields( RowMetaInterface rmi, List<SequoiaDBOutputFieldInfo> fields ) throws KettleException {
      // check if match input fields
      if ( rmi.getFieldNames().length <= 0 ) {
         throw new KettleException( BaseMessages.getString( PKG,
               "SequoiaDBOutput.Msg.Err.InputFieldsSizeError" )) ;
      }
      if( fields.size() <= 0 ){
         throw new KettleException( BaseMessages.getString( PKG,
               "SequoiaDBOutput.Msg.Err.OutputFieldsEmpty" )) ;
      }
      
      Set<String> input = new HashSet<String>( rmi.getFieldNames().length, 1 ) ;
      Set<String> output = new HashSet<String>( fields.size(), 1 ) ;
      for( int i = 0; i < rmi.size(); i++ ) {
         input.add( rmi.getValueMeta( i ).getName() );
      }
      for( int j = 0; j < fields.size(); j++ ){
         output.add( fields.get(j).getName() ) ;
      }
      
      if ( !input.containsAll( output )) {
         output.removeAll( input ) ;
         StringBuffer loseFields = new StringBuffer() ;
         for( String name : output ) {
            loseFields.append( "'" ).append( name ).append( "', ") ;
         }
         throw new KettleException( BaseMessages.getString( PKG,
               "SequoiaDBOutput.Msg.Err.FieldsNotFoundInInput", loseFields.toString() ));
      }
   }
}
