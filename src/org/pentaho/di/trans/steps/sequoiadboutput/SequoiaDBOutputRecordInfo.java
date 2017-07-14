package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;

public class SequoiaDBOutputRecordInfo{
   
   private List<SequoiaDBOutputFieldInfo> m_fields = new ArrayList<SequoiaDBOutputFieldInfo>();
   
   private SequoiaDBInsertRecordInfo m_insertRecInfo = new SequoiaDBInsertRecordInfo() ;
   
   private SequoiaDBUpdateRecordInfo m_updateRecInfo = new SequoiaDBUpdateRecordInfo() ;
   
   public List<SequoiaDBOutputFieldInfo> getFieldsInfo(){
      return m_fields ;
   }
   
   public void addField( SequoiaDBOutputFieldInfo fieldInfo ) throws KettleValueException{
      m_fields.add( fieldInfo ) ;
      m_insertRecInfo.addField( fieldInfo ) ;
      m_updateRecInfo.addField( fieldInfo ) ;
   }
   
   public void done() throws KettleValueException{
      m_insertRecInfo.done();
      m_updateRecInfo.done();
   }
   
   public BSONObject getInserter( Object[] row, RowMetaInterface rowMeta ) throws KettleValueException {
      BSONObject inserter = m_insertRecInfo.toBson( row, rowMeta ) ;
      return inserter ;
   }
   
   public BSONObject getUpdater( Object[] row, RowMetaInterface rowMeta ) throws KettleValueException {
      BSONObject updater = m_updateRecInfo.getUpdater( row, rowMeta ) ;
      return updater ;
   }
   
   public BSONObject getUpdateCond( Object[] row, RowMetaInterface rowMeta ) throws KettleValueException {
      BSONObject condition = m_updateRecInfo.getUpdateCond( row, rowMeta ) ;
      return condition;
   }
}