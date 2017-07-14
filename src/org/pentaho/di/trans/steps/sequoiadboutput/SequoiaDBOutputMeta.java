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

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.sequoiadb.SequoiaDBMeta;
import org.pentaho.di.ui.trans.steps.sequoiadboutput.SequoiaDBOutputDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step(   
      id = "SequoiaDBOutput",
      image = "org/pentaho/di/trans/steps/import.png",
      i18nPackageName="org.pentaho.di.trans.steps.sequoiadboutput",
      name="SequoiaDBOutput.Name",
      description = "SequoiaDBOutput.TooltipDesc",
      categoryDescription = "SequoiaDBOutput.categoryDescription"
)

public class SequoiaDBOutputMeta extends SequoiaDBMeta {

   private static Class<?> PKG = SequoiaDBOutputMeta.class;// for i18n purposes

   private SequoiaDBOutputRecordInfo m_fields = null;
   
   private static int BULK_INSERT_SIZE_DFT = 1000;

   private int m_bulkInsertSize = BULK_INSERT_SIZE_DFT;
   
   private boolean m_truncate = false;
   
   private boolean m_update = false;
   
   private boolean m_upsert = false;

   public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
      return new SequoiaDBOutputDialog(shell, meta, transMeta, name);
   }

   @Override
   public Object clone() {
      SequoiaDBOutputMeta retval =(SequoiaDBOutputMeta) super.clone();
      retval.m_fields = m_fields;
      retval.m_bulkInsertSize = m_bulkInsertSize;
      retval.m_truncate = m_truncate ;
      retval.m_update = m_update ;
      retval.m_upsert = m_upsert ;
      return retval;
   }

   @Override
   public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
         int cnr, TransMeta transMeta, Trans disp) {
      return new SequoiaDBOutput(stepMeta, stepDataInterface, cnr, transMeta, disp);
   }

   @Override
   public StepDataInterface getStepData() {
      return new SequoiaDBOutputData();
   }

   @Override
   public void setDefault() {
      // TODO Auto-generated method stub
      
   }

   @Override
   public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException{
      rep.saveStepAttribute( id_transformation, id_step, "hostname", getHostname() );
      rep.saveStepAttribute( id_transformation, id_step, "port", getPort() );
      rep.saveStepAttribute( id_transformation, id_step, "sequoiadbusername", getUserName() );
      rep.saveStepAttribute( id_transformation, id_step, "sequoiadbpassword", getPwd() );
      rep.saveStepAttribute( id_transformation, id_step, "CSName", getCSName() );
      rep.saveStepAttribute( id_transformation, id_step, "CLName", getCLName() );
      rep.saveStepAttribute( id_transformation, id_step, "bulkinsertsize", getBulkInsertSizeStr() );
      rep.saveStepAttribute( id_transformation, id_step, "truncate", getTruncate() );
      rep.saveStepAttribute( id_transformation, id_step, "update", getUpdate() );
      rep.saveStepAttribute( id_transformation, id_step, "upsert", getUpsert() );
      
      List<SequoiaDBOutputFieldInfo> fieldsInfo = m_fields.getFieldsInfo() ;
      if ( fieldsInfo != null ){
         int fieldNum = fieldsInfo.size() ;
         SequoiaDBOutputFieldInfo tmpFieldInfo ;
         for( int i = 0 ; i < fieldNum ; i++ ) {
            tmpFieldInfo = fieldsInfo.get(i) ;
            rep.saveStepAttribute( id_transformation, id_step, i, "field_name",
                                   tmpFieldInfo.getName());
            rep.saveStepAttribute( id_transformation, id_step, i, "field_path",
                                   tmpFieldInfo.getPath());
            rep.saveStepAttribute( id_transformation, id_step, i, "field_cond",
                                   tmpFieldInfo.getCond());
            rep.saveStepAttribute( id_transformation, id_step, i, "field_updateop",
                                   tmpFieldInfo.getUpdateOp());
         }
      }
   }

   @Override
   public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
     throws KettleException {
      setHostname( rep.getStepAttributeString( id_step, "hostname" ) );
      setPort( rep.getStepAttributeString( id_step, "port" ) );
      setUserName( rep.getStepAttributeString( id_step, "sequoiadbusername" ) );
      setPwd( rep.getStepAttributeString( id_step, "sequoiadbpassword" ) );
      setCSName( rep.getStepAttributeString( id_step, "CSName" ) );
      setCLName( rep.getStepAttributeString( id_step, "CLName" ) );
      setBulkInsertSize( rep.getStepAttributeString( id_step, "bulkinsertsize" ) );
      setTruncate( rep.getStepAttributeBoolean( id_step, "truncate" ) );
      setUpdate( rep.getStepAttributeBoolean( id_step, "update" ) );
      setUpsert( rep.getStepAttributeBoolean( id_step, "upsert" ) );

      int numFields = rep.countNrStepAttributes(id_step, "field_name");
      if(numFields > 0){
         m_fields = new SequoiaDBOutputRecordInfo();
         for( int i = 0; i < numFields; i++){
            SequoiaDBOutputFieldInfo fieldInfo
                     = new SequoiaDBOutputFieldInfo(rep.getStepAttributeString(id_step, i, "field_name"),
                                                    rep.getStepAttributeString(id_step, i, "field_path")) ;
            fieldInfo.setCond( rep.getStepAttributeBoolean(id_step, i, "field_cond") ) ;
            fieldInfo.setUpdateOp( rep.getStepAttributeString(id_step, i, "field_updateop") ) ;
            m_fields.addField( fieldInfo );
         }
         m_fields.done() ;
      }
   }

   @Override
   public String getXML() {
     StringBuffer retval = new StringBuffer( 300 );
     if ( null != getHostname() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "hostname", getHostname() ) );
     }

     if ( null != getPort() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "port", getPort() ) );
     }

     if ( null != getUserName() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "sequoiadbusername", getUserName() ) );
     }

     if ( null != getPwd() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "sequoiadbpassword", getPwd() ) );
     }

     if ( null != getCSName() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "CSName", getCSName() ) );
     }

     if ( null != getCLName() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "CLName", getCLName() ) );
     }

     retval.append( "    " ).append( XMLHandler.addTagValue( "bulkinsertsize", getBulkInsertSizeStr() ) );
     
     retval.append( "    " ).append( XMLHandler.addTagValue( "truncate", getTruncate() ) );
     
     retval.append( "    " ).append( XMLHandler.addTagValue( "update", getUpdate() ) );
     
     retval.append( "    " ).append( XMLHandler.addTagValue( "upsert", getUpsert() ) );

     List<SequoiaDBOutputFieldInfo> fieldsInfo = m_fields.getFieldsInfo() ;
     if ( fieldsInfo != null ){
        retval.append( "    ").append( XMLHandler.openTag( "selected_fields" ));
        
        int fieldNum = fieldsInfo.size() ;
        SequoiaDBOutputFieldInfo tmpFieldInfo ;
        for( int i = 0 ; i < fieldNum ; i++ ) {
           tmpFieldInfo = fieldsInfo.get(i) ;
           retval.append("      ").append(XMLHandler.openTag( "selected_field" ));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_name", tmpFieldInfo.getName()));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_path", tmpFieldInfo.getPath()));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_cond", tmpFieldInfo.getCond()));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_updateop", tmpFieldInfo.getUpdateOp()));
           retval.append("      ").append(XMLHandler.closeTag( "selected_field" ));
        }
        retval.append("    ").append(XMLHandler.closeTag( "selected_fields" ));
     }
     return retval.toString();
   }

   @Override
   public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
     String strTmp;
     strTmp = XMLHandler.getTagValue( stepnode, "hostname" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setHostname( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "port" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setPort( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "sequoiadbusername" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setUserName( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "sequoiadbpassword" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setPwd( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "CSName" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setCSName( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "CLName" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setCLName( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "bulkinsertsize" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setBulkInsertSize( strTmp );
     }
     
     strTmp = XMLHandler.getTagValue( stepnode, "truncate" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setTruncate(strTmp.equalsIgnoreCase( "Y" ));
     }
     
     strTmp = XMLHandler.getTagValue( stepnode, "update" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setUpdate(strTmp.equalsIgnoreCase( "Y" ));
     }
     
     strTmp = XMLHandler.getTagValue( stepnode, "upsert" );
     if ( null != strTmp && !strTmp.isEmpty())
     {
        setUpsert(strTmp.equalsIgnoreCase( "Y" ));
     }

     Node selectedFields = XMLHandler.getSubNode( stepnode, "selected_fields");
     if ( selectedFields != null && XMLHandler.countNodes(selectedFields, "selected_field") > 0 ){
        int numFields = XMLHandler.countNodes(selectedFields, "selected_field");
        m_fields = new SequoiaDBOutputRecordInfo();
        for ( int i = 0; i < numFields; i++ ){
           Node fieldNode = XMLHandler.getSubNodeByNr( selectedFields, "selected_field", i);
           try {
              SequoiaDBOutputFieldInfo tmpFieldInfo
                       = new SequoiaDBOutputFieldInfo(XMLHandler.getTagValue( fieldNode, "field_name"),
                                                      XMLHandler.getTagValue( fieldNode, "field_path") );
              strTmp = XMLHandler.getTagValue( fieldNode, "field_cond" );
              if ( null != strTmp && !strTmp.isEmpty())
              {
                 tmpFieldInfo.setCond(strTmp.equalsIgnoreCase( "Y" ));
              }
              strTmp = XMLHandler.getTagValue( fieldNode, "field_updateop" );
              if ( null != strTmp && !strTmp.isEmpty())
              {
                 tmpFieldInfo.setUpdateOp(strTmp);
              }
              m_fields.addField( tmpFieldInfo );
           } catch (KettleValueException e) {
              e.printStackTrace();
              throw new KettleXMLException( BaseMessages.getString( PKG,
                                            "SequoiaDBOutput.Msg.Err.FailedToAddField",
                                            XMLHandler.getTagValue( fieldNode, "field_name"),
                                            XMLHandler.getTagValue( fieldNode, "field_path") ));
           }
        }
        try {
         m_fields.done();
      } catch (KettleValueException e) {
         throw new KettleXMLException( e.toString() );
      }
     }
   }
   
   public void addSelectedField( String srcFieldName, String fieldPath,
                                 String isCond, String updateOp) throws KettleValueException{
      if ( null == m_fields ){
         m_fields = new SequoiaDBOutputRecordInfo();
      }
      SequoiaDBOutputFieldInfo tmpField = new SequoiaDBOutputFieldInfo( srcFieldName, fieldPath );
      tmpField.setCond( isCond.equalsIgnoreCase( "Y" ));
      tmpField.setUpdateOp( updateOp );
      m_fields.addField( tmpField );
   }
   
   public void done() throws KettleValueException{
      m_fields.done() ;
   }
   
   public List<SequoiaDBOutputFieldInfo> getSelectedFields(){
      if ( null == m_fields ){
         return null ;
      }
      return m_fields.getFieldsInfo() ;
   }
   
   public SequoiaDBOutputRecordInfo getOutputRecordInfo(){
      return m_fields ;
   }
   
   public void clearFields(){
      m_fields = new SequoiaDBOutputRecordInfo();
   }
   
   public void setBulkInsertSize(String BulkInsertSize ){
      try{
         Integer tmp = Integer.valueOf(BulkInsertSize);
         if (tmp != null){
            m_bulkInsertSize = tmp.intValue() ;
         }
      }
      catch(NumberFormatException e){
         // Do nothing. Use the old value if input error.
      }
      if ( m_bulkInsertSize <= 0 ){
         m_bulkInsertSize = BULK_INSERT_SIZE_DFT ;
      }
   }
   
   public int getBulkInsertSize(){
      return m_bulkInsertSize ;
   }
   
   public String getBulkInsertSizeStr(){
      return Integer.toString( m_bulkInsertSize ) ;
   }
   
   public void setTruncate(boolean truncate) {
      m_truncate = truncate;
   }
   
   public boolean getTruncate() {
      return m_truncate;
   }
   
   public void setUpdate(boolean update) {
      m_update = update;
   }
   
   public boolean getUpdate() {
      return m_update;
   }
   
   public void setUpsert(boolean upsert) {
      m_upsert = upsert;
   }
   
   public boolean getUpsert() {
      return m_upsert;
   }
}
