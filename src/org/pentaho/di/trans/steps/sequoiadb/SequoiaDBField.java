package org.pentaho.di.trans.steps.sequoiadb;

import org.pentaho.di.core.exception.KettleException;



public class SequoiaDBField implements Comparable<SequoiaDBField> {

   public String m_fieldName = "";
   public String m_kettleType = "";
   public String m_path = "";
   public int m_outputIndex = 0;

   @Override
   public int compareTo( SequoiaDBField comp ) {
     return m_fieldName.compareTo( comp.m_fieldName );
   }
}