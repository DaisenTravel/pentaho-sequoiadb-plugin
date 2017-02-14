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
import java.util.Map;

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

   public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
      return new SequoiaDBOutputDialog(shell, meta, transMeta, name);
   }

   @Override
   public Object clone() {
      SequoiaDBOutputMeta retval =(SequoiaDBOutputMeta) super.clone();
      retval.m_fields = m_fields;
      retval.m_bulkInsertSize = m_bulkInsertSize;
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
      
      Map<String, SequoiaDBOutputFieldInfo> fieldsInfo = m_fields.getFieldsInfo() ;
      if ( fieldsInfo != null && fieldsInfo.size() > 0 ){
         int i = 0 ;
         for( Map.Entry<String, SequoiaDBOutputFieldInfo> entry : fieldsInfo.entrySet() ){
            List<String> fieldPathList = entry.getValue().getFieldPath() ;
            int fieldPathNum = fieldPathList.size() ;
            for( int j = 0 ; j < fieldPathNum ; j++ ){
               rep.saveStepAttribute( id_transformation, id_step, i, "field_name",
                                      entry.getKey() );
               rep.saveStepAttribute( id_transformation, id_step, i, "field_path",
                                      fieldPathList.get(j) );
               ++i ;
            }
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

      int numFields = rep.countNrStepAttributes(id_step, "field_name");
      if(numFields > 0){
         m_fields = new SequoiaDBOutputRecordInfo();
         for( int i = 0; i < numFields; i++){
            m_fields.addField( rep.getStepAttributeString(id_step, i, "field_name"),
                  rep.getStepAttributeString(id_step, i, "field_path") );
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

     Map<String, SequoiaDBOutputFieldInfo> fieldsInfo = m_fields.getFieldsInfo() ;
     if ( fieldsInfo != null && fieldsInfo.size() > 0 ){
        retval.append( "    ").append( XMLHandler.openTag( "selected_fields" ));
        for( Map.Entry<String, SequoiaDBOutputFieldInfo> entry : fieldsInfo.entrySet() ){
           List<String> fieldPathList = entry.getValue().getFieldPath() ;
           int fieldPathNum = fieldPathList.size() ;
           for( int j = 0 ; j < fieldPathNum ; j++ ){
              retval.append("      ").append(XMLHandler.openTag( "selected_field" ));
              retval.append("        ").append(
                    XMLHandler.addTagValue( "field_name", entry.getKey()));
              retval.append("        ").append(
                    XMLHandler.addTagValue( "field_path", fieldPathList.get(j)));
              retval.append("      ").append(XMLHandler.closeTag( "selected_field" ));
           }
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
     
     Node selectedFields = XMLHandler.getSubNode( stepnode, "selected_fields");
     if ( selectedFields != null && XMLHandler.countNodes(selectedFields, "selected_field") > 0 ){
        int numFields = XMLHandler.countNodes(selectedFields, "selected_field");
        m_fields = new SequoiaDBOutputRecordInfo();
        for ( int i = 0; i < numFields; i++ ){
           Node fieldNode = XMLHandler.getSubNodeByNr( selectedFields, "selected_field", i);
           try {
              m_fields.addField( XMLHandler.getTagValue( fieldNode, "field_name"),
                                 XMLHandler.getTagValue( fieldNode, "field_path") );
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
   
   public void addSelectedField( String srcFieldName, String fieldPath ) throws KettleValueException{
      if ( null == m_fields ){
         m_fields = new SequoiaDBOutputRecordInfo();
      }
      m_fields.addField( srcFieldName, fieldPath );
   }
   
   public void done() throws KettleValueException{
      m_fields.done() ;
   }
   
   public Map<String, SequoiaDBOutputFieldInfo> getSelectedFields(){
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
}
