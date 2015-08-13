package org.pentaho.di.trans.steps.sequoiadboutput;


import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
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

   private List<SequoiaDBOutputField> m_fields;

   public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
      return new SequoiaDBOutputDialog(shell, meta, transMeta, name);
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
      rep.saveStepAttribute( id_transformation, id_step, "CSName", getCSName() );
      rep.saveStepAttribute( id_transformation, id_step, "CLName", getCLName() );
      
      if ( m_fields != null && m_fields.size() > 0 ){
         for ( int i = 0; i < m_fields.size(); i++ ){
            SequoiaDBOutputField fieldTmp = m_fields.get(i);
            rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldTmp.m_fieldName);
            rep.saveStepAttribute(id_transformation, id_step, i, "field_path", fieldTmp.m_path);
         }
      }
   }

   @Override
   public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
     throws KettleException {
      setHostname( rep.getStepAttributeString( id_step, "hostname" ) );
      setPort( rep.getStepAttributeString( id_step, "port" ) );
      setCSName( rep.getStepAttributeString( id_step, "CSName" ) );
      setCLName( rep.getStepAttributeString( id_step, "CLName" ) );

      int numFields = rep.countNrStepAttributes(id_step, "field_name");
      if(numFields > 0){
         m_fields = new ArrayList<SequoiaDBOutputField>();
         for( int i = 0; i < numFields; i++){
            SequoiaDBOutputField fieldTmp = new SequoiaDBOutputField();
            fieldTmp.m_fieldName = rep.getStepAttributeString(id_step, i, "field_name");
            fieldTmp.m_path = rep.getStepAttributeString(id_step, i, "field_path");
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

     if ( null != getCSName() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "CSName", getCSName() ) );
     }

     if ( null != getCLName() )
     {
        retval.append( "    " ).append( XMLHandler.addTagValue( "CLName", getCLName() ) );
     }

     if ( m_fields != null && m_fields.size() > 0 ){
        retval.append( "\n    ").append( XMLHandler.openTag( "selected_fields" ));
        for ( SequoiaDBOutputField f : m_fields ){
           retval.append("\n      ").append(XMLHandler.openTag( "selected_field" ));
           retval.append("\n        ").append(
                 XMLHandler.addTagValue( "field_name", f.m_fieldName));
           retval.append("\n        ").append(
                 XMLHandler.addTagValue( "field_path", f.m_path));
           retval.append("\n      ").append(XMLHandler.closeTag( "selected_field" ));
        }
        retval.append("\n    ").append(XMLHandler.closeTag( "selected_fields" ));
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
     
     Node selectedFields = XMLHandler.getSubNode( stepnode, "selected_fields");
     if ( selectedFields != null && XMLHandler.countNodes(selectedFields, "selected_field") > 0 ){
        int numFields = XMLHandler.countNodes(selectedFields, "selected_field");
        
        m_fields = new ArrayList<SequoiaDBOutputField>();
        for ( int i = 0; i < numFields; i++ ){
           Node fieldNode = XMLHandler.getSubNodeByNr( selectedFields, "selected_field", i);
           SequoiaDBOutputField fieldTmp = new SequoiaDBOutputField();
           fieldTmp.m_fieldName = XMLHandler.getTagValue( fieldNode, "field_name");
           fieldTmp.m_path = XMLHandler.getTagValue( fieldNode, "field_path");
           m_fields.add(fieldTmp);
        }
     }
   }
   
   public void setSelectedFields(List<SequoiaDBOutputField> fields){
      m_fields = fields;
   }

   public List<SequoiaDBOutputField> getSelectedFields(){
      return m_fields;
   }
   
}