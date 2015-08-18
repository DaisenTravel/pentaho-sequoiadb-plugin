package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BSONObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
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

   // TODO: get bulkInsertSize from dialog
   protected int m_bulkInsertSize = 100 ;
   private List<BSONObject> m_buffer ;

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
         sdb = new Sequoiadb(connString, "", "");
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

         // clone the input row structure and place it in our data object
         m_buffer = new ArrayList< BSONObject >( m_bulkInsertSize ) ;

         // output the same as the input
         RowMetaInterface rmi = getInputRowMeta() ;
         m_data.setOutputRowMeta( rmi ) ;

         // check if match the input fields
         List<SequoiaDBOutputField> selectedFields = m_meta.getSelectedFields();
         if ( null != selectedFields ) {
            checkFields( rmi, selectedFields ) ;
         }
      }

      // log progress if it is time to to so
      if (checkFeedback(getLinesRead())) {
         logBasic("Linenr " + getLinesRead()); // Some basic logging
      }
      List<SequoiaDBOutputField> l = m_meta.getSelectedFields() ;
      if ( null != l && 0 != l.size() ) {
         BSONObject recObj = m_data.kettleToBson( r,
                                                  m_meta.getSelectedFields() );
         if ( recObj != null ) {
             m_buffer.add( recObj ) ;
          }

          if ( m_buffer.size() >= m_bulkInsertSize ){
             flushToDB() ;
          }
      }   

      // indicate that processRow() should be called again
      return true;
   }
   
   void flushToDB() {
      if ( m_buffer.size() > 0 ){
         m_cl.bulkInsert( m_buffer, 0  );
         m_buffer.clear();
      }
   }
   
   void checkFields( RowMetaInterface rmi, List<SequoiaDBOutputField> fields ) throws KettleException {
      // check if match input fields
      if ( rmi.getFieldNames().length <= 0 || rmi.getFieldNames().length < fields.size() ) {
         throw new KettleException( BaseMessages.getString( PKG,
               "SequoiaDBOutput.Msg.Err.InputFieldsSizeError" )) ;
      }
      
      if ( conflict(fields) ) {
    	  throw new KettleException( BaseMessages.getString( PKG,
                  "SequoiaDBOutput.Msg.Err.FailedToWriteTheFieldVal"
                  + "the field path is conflict" ) );
      }
      
      Set<String> input = new HashSet<String>( rmi.getFieldNames().length, 1 ) ;
      Set<String> output = new HashSet<String>( fields.size(), 1 ) ;
      for( int i = 0; i < rmi.size(); i++ ) {
         input.add( rmi.getValueMeta( i ).getName() );
      }
      for ( SequoiaDBOutputField f : fields ) {
         output.add( f.m_fieldName ) ;
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
   
   public boolean conflict(List<SequoiaDBOutputField> fields) {
	   List<String> pathField = new ArrayList<String>();
	   for ( SequoiaDBOutputField row : fields) {
		   pathField.add(row.m_path);
	   }
	   
	   if ( !pathField.isEmpty() && pathField.size() >= 2 ) {
		   int size = pathField.size();
		   for (int i = 0; i < size; ++i) {
			   for(int j = i+1; j < size; ++j) {
				   if (    pathField.get(i).indexOf(pathField.get(j)) == 0
						|| pathField.get(j).indexOf(pathField.get(i)) == 0) {
					   return true;
				   }
			   }
		   }
	   }
		   
	   return false;
   }
}
