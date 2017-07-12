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
package org.pentaho.di.trans.steps.sequoiadbinput;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
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
import org.pentaho.di.ui.trans.steps.sequoiadbinput.SequoiaDBInputDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step(	
		id = "SequoiaDBInput",
		image = "org/pentaho/di/trans/steps/export.png",
		i18nPackageName="org.pentaho.di.TRANS.steps.sequoiadbinput",
		name="SequoiaDBInput.Name",
		description = "SequoiaDBInput.TooltipDesc",
		categoryDescription = "SequoiaDBInput.categoryDescription"
)

public class SequoiaDBInputMeta extends SequoiaDBMeta {

   private static Class<?> PKG = SequoiaDBInputMeta.class;// for i18n purposes

   private List<SequoiaDBInputField> m_fields;
   private String m_query = "";
   private String m_selector = "";
   private String m_orderby = "";
   private int m_skip = 0;
   private int m_limit = -1;
   
   public void init( RowMetaInterface outputRowMeta) throws KettlePluginException{
      if ( null != m_fields ) {
         for( SequoiaDBInputField f:m_fields){
            int outputIndex = outputRowMeta.indexOfValue( f.m_fieldName );
            f.init( outputIndex );
         }
      }
   }

   public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
      return new SequoiaDBInputDialog(shell, meta, transMeta, name);
   }

   @Override
   public Object clone() {
      SequoiaDBInputMeta retval =(SequoiaDBInputMeta) super.clone();
      retval.m_query = m_query;
      retval.m_selector = m_selector;
      retval.m_orderby = m_orderby;
      retval.m_skip = m_skip;
      retval.m_limit = m_limit;
      return retval;
   }

   @Override
   public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
         int cnr, TransMeta transMeta, Trans disp) {
      return new SequoiaDBInput(stepMeta, stepDataInterface, cnr, transMeta, disp);
   }

   @Override
   public StepDataInterface getStepData() {
      return new SequoiaDBInputData();
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
      rep.saveStepAttribute( id_transformation, id_step, "query", getQuery() );
      rep.saveStepAttribute( id_transformation, id_step, "selector", getSelector() );
      rep.saveStepAttribute( id_transformation, id_step, "orderby", getOrderby() );
      rep.saveStepAttribute( id_transformation, id_step, "skip", getSkipStr() );
      rep.saveStepAttribute( id_transformation, id_step, "limit", getLimitStr() );
      
      if (m_fields != null && m_fields.size() > 0){
         for (int i = 0; i < m_fields.size(); i++){
            SequoiaDBInputField fieldTmp = m_fields.get(i);
            rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldTmp.m_fieldName);
            rep.saveStepAttribute(id_transformation, id_step, i, "field_path", fieldTmp.m_path);
            rep.saveStepAttribute(id_transformation, id_step, i, "field_type", fieldTmp.m_kettleType);
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
      setQuery( rep.getStepAttributeString( id_step, "query" ) );
      setSelector( rep.getStepAttributeString( id_step, "selector" ) );
      setOrderby( rep.getStepAttributeString( id_step, "orderby" ) );
      setSkip( rep.getStepAttributeString( id_step, "skip" ) );
      setLimit( rep.getStepAttributeString( id_step, "limit" ) );

      int numFields = rep.countNrStepAttributes(id_step, "field_name");
      if(numFields > 0){
         m_fields = new ArrayList<SequoiaDBInputField>();
         for( int i = 0; i < numFields; i++){
            SequoiaDBInputField fieldTmp = new SequoiaDBInputField();
            fieldTmp.m_fieldName = rep.getStepAttributeString(id_step, i, "field_name");
            fieldTmp.m_path = rep.getStepAttributeString(id_step, i, "field_path");
            fieldTmp.m_kettleType = rep.getStepAttributeString(id_step, i, "field_type");
            m_fields.add(fieldTmp);
         }
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

     if ( null != getQuery() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "query", getQuery() ) );
     }

     if ( null != getSelector() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "selector", getSelector() ) );
     }

     if ( null != getOrderby() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "orderby", getOrderby() ) );
     }

     if ( null != getSkipStr() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "skip", getSkipStr() ) );
     }

     if ( null != getLimitStr() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "limit", getLimitStr() ) );
     }

     if ( m_fields != null && m_fields.size() > 0 ){
        retval.append( "    ").append( XMLHandler.openTag( "selected_fields" ));
        for ( SequoiaDBInputField f : m_fields ){
           retval.append("      ").append(XMLHandler.openTag( "selected_field" ));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_name", f.m_fieldName));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_path", f.m_path));
           retval.append("        ").append(
                 XMLHandler.addTagValue( "field_type", f.m_kettleType));
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
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setHostname( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "port" );
     if ( strTmp != null && !strTmp.isEmpty())
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
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setCSName( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "CLName" );
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setCLName( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "query" );
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setQuery( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "selector" );
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setSelector( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "orderby" );
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setOrderby( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "skip" );
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setSkip( strTmp );
     }
     strTmp = XMLHandler.getTagValue( stepnode, "limit" );
     if ( strTmp != null && !strTmp.isEmpty())
     {
        setLimit( strTmp );
     }
     
     Node selectedFields = XMLHandler.getSubNode( stepnode, "selected_fields");
     if ( selectedFields != null && XMLHandler.countNodes(selectedFields, "selected_field") > 0 ){
        int numFields = XMLHandler.countNodes(selectedFields, "selected_field");
        
        m_fields = new ArrayList<SequoiaDBInputField>();
        for ( int i = 0; i < numFields; i++ ){
           Node fieldNode = XMLHandler.getSubNodeByNr( selectedFields, "selected_field", i);
           SequoiaDBInputField fieldTmp = new SequoiaDBInputField();
           fieldTmp.m_fieldName = XMLHandler.getTagValue( fieldNode, "field_name");
           fieldTmp.m_path = XMLHandler.getTagValue( fieldNode, "field_path");
           fieldTmp.m_kettleType = XMLHandler.getTagValue( fieldNode, "field_type");
           m_fields.add(fieldTmp);
        }
     }
   }

   @SuppressWarnings( "deprecation" )
   @Override
   public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
         VariableSpace space ) throws KettleStepException {
      try {
				if ( m_fields == null || m_fields.size() == 0 ){
				   // TODO: get the name "json" from dialog
					
				   ValueMetaInterface jsonValueMeta = ValueMetaFactory.createValueMeta("JSON", ValueMetaInterface.TYPE_STRING);
				   jsonValueMeta.setOrigin( origin );
				   rowMeta.addValueMeta( jsonValueMeta );
				}else{
				   // get the selected fields
				   for ( SequoiaDBInputField f : m_fields ){
				      ValueMetaInterface vm = ValueMetaFactory.createValueMeta(f.m_fieldName, ValueMetaFactory.getIdForValueMeta( f.m_kettleType ));
				      vm.setOrigin( origin );
				      rowMeta.addValueMeta( vm );
				   }
				}
			} catch (KettlePluginException e) {
				throw new KettleStepException(e);
			}
   }
   
   public void setSelectedFields(List<SequoiaDBInputField> fields){
      m_fields = fields;
   }

   public List<SequoiaDBInputField> getSelectedFields(){
      return m_fields;
   }
   
   public String getQuery() {
      return m_query;
   }
   
   public void setQuery(String query) {
      this.m_query = query;
   }
   
   public String getSelector() {
      return m_selector;
   }
   
   public void setSelector(String selector) {
      this.m_selector = selector;
   }
   
   public String getOrderby() {
      return m_orderby;
   }
   
   public void setOrderby(String orderby) {
      this.m_orderby = orderby;
   }
   
   public String getSkipStr() {
      return Integer.toString(m_skip);
   }
   
   public int getSkip() {
      return m_skip;
   }
   
   public void setSkip(String skip) {
      try{
         Integer tmp = Integer.valueOf(skip);
         if (tmp != null){
            m_skip = tmp.intValue() ;
         }
      }
      catch(NumberFormatException e){
         // Do nothing. Use the old value if input error.
      }
      if ( m_skip < 0 ){
         m_skip = 0 ;
      }
   }
   
   public String getLimitStr() {
      return Integer.toString(m_limit);
   }
   
   public int getLimit() {
      return m_limit;
   }
   
   public void setLimit(String limit) {
      try{
         Integer tmp = Integer.valueOf(limit);
         if (tmp != null){
            m_limit = tmp.intValue() ;
         }
      }
      catch(NumberFormatException e){
         // Do nothing. Use the old value if input error.
      }
      if ( m_limit < 0 ){
         m_limit = -1 ;
      }
   }
}
