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
package org.pentaho.di.ui.trans.steps.sequoiadbinput;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.sequoiadbinput.SequoiaDBInputField;
import org.pentaho.di.trans.steps.sequoiadbinput.SequoiaDBInputMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class SequoiaDBInputDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = SequoiaDBInputMeta.class;
	private CTabFolder m_wTabFolder;
	private CTabItem m_wSdbConnectionTab;
	private CTabItem m_wSdbInputTab;
	private CTabItem m_wSdbQueryTab;
	private CTabItem m_wSdbFieldsTab;

	private TextVar m_wHostname;
	private TextVar m_wPort;
   private TextVar m_wUsername;
   private TextVar m_wPassword;
	private TextVar m_wCSName;
	private TextVar m_wCLName;
	private StyledTextComp m_wQuery;
   private TextVar m_wOrderby;
   private TextVar m_wSkip;
   private TextVar m_wLimit;
	private TableView m_fieldsView;

	private SequoiaDBInputMeta m_meta;
	
	private final int FIRST_COL = 1;
	private final int SECOND_COL = 2;
	private final int THIRD_COL = 3;
	
	public SequoiaDBInputDialog(Shell parent, Object in,
			TransMeta transMeta, String stepname) {
		super(parent, (BaseStepMeta) in, transMeta, stepname);
		m_meta = (SequoiaDBInputMeta) in ;
	}

	@Override
	public String open() {

      // store some convenient SWT variables 
      Shell parent = getParent();
      Display display = parent.getDisplay();

      // SWT code for preparing the dialog
      shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
      props.setLook(shell);
      setShellImage(shell, m_meta);
      
      // Save the value of the changed flag on the meta object. If the user cancels
      // the dialog, it will be restored to this saved value.
      // The "changed" variable is inherited from BaseStepDialog
      changed = m_meta.hasChanged();
      
      // The ModifyListener used on all controls. It will update the meta object to 
      // indicate that changes are being made.
      ModifyListener lsMod = new ModifyListener() {
         public void modifyText(ModifyEvent e) {
            m_meta.setChanged();
         }
      };
      
      // ------------------------------------------------------- //
      // SWT code for building the actual settings dialog        //
      // ------------------------------------------------------- //
      FormLayout formLayout = new FormLayout();
      formLayout.marginWidth = Const.FORM_MARGIN;
      formLayout.marginHeight = Const.FORM_MARGIN;

      shell.setLayout(formLayout);
      shell.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Shell.Title")); 

      int middle = props.getMiddlePct();
      int margin = Const.MARGIN;

      // Stepname line
      wlStepname = new Label(shell, SWT.RIGHT);
      wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName")); 
      props.setLook(wlStepname);
      fdlStepname = new FormData();
      fdlStepname.left = new FormAttachment(0, 0);
      fdlStepname.right = new FormAttachment(middle, -margin);
      fdlStepname.top = new FormAttachment(0, margin);
      wlStepname.setLayoutData(fdlStepname);
      
      wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      wStepname.setText(stepname);
      props.setLook(wStepname);
      wStepname.addModifyListener(lsMod);
      fdStepname = new FormData();
      fdStepname.left = new FormAttachment(middle, 0);
      fdStepname.top = new FormAttachment(0, margin);
      fdStepname.right = new FormAttachment(100, 0);
      wStepname.setLayoutData(fdStepname);
      Control lastControl = wStepname;

      m_wTabFolder = new CTabFolder(shell, SWT.BORDER);
      props.setLook(m_wTabFolder, Props.WIDGET_STYLE_TAB);
      m_wTabFolder.setSimple(false);

      // *************Connection tab***********
      m_wSdbConnectionTab = new CTabItem(m_wTabFolder, SWT.NONE);
      m_wSdbConnectionTab.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.ConnectionTab.Title")) ;
      Composite wConnComp = new Composite(m_wTabFolder, SWT.NONE);
      props.setLook(wConnComp);
      FormLayout connLayout = new FormLayout();
      connLayout.marginWidth = 3;
      connLayout.marginHeight = 3;
      wConnComp.setLayout(connLayout);
      
      // Hostname
      Label wlHostname = new Label(wConnComp, SWT.RIGHT);
      wlHostname.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.Hostname.Label"));
      props.setLook(wlHostname);
      FormData fdlHostname = new FormData();
      fdlHostname.left = new FormAttachment(0, 0);
      fdlHostname.right = new FormAttachment(middle, -margin);
      fdlHostname.top = new FormAttachment(0, margin);
      wlHostname.setLayoutData(fdlHostname);
      m_wHostname = new TextVar(transMeta, wConnComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wHostname);
      m_wHostname.addModifyListener(lsMod);
      FormData fdHostname = new FormData();
      fdHostname.left = new FormAttachment(middle, 0);
      fdHostname.top = new FormAttachment(0, margin);
      fdHostname.right = new FormAttachment(100, 0);
      m_wHostname.setLayoutData(fdHostname);
      lastControl = m_wHostname;
      
      // Port
      Label wlPort = new Label(wConnComp, SWT.RIGHT);
      wlPort.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Port.Label"));
      props.setLook(wlPort);
      FormData fdlPort = new FormData();
      fdlPort.left = new FormAttachment(0, 0);
      fdlPort.right = new FormAttachment(middle, -margin);
      fdlPort.top = new FormAttachment(lastControl, margin);
      wlPort.setLayoutData(fdlPort);
      m_wPort = new TextVar(transMeta, wConnComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wPort);
      m_wPort.addModifyListener(lsMod);
      FormData fdPort = new FormData();
      fdPort.left = new FormAttachment(middle, 0);
      fdPort.top = new FormAttachment(lastControl, margin);
      fdPort.right = new FormAttachment(100, 0);
      m_wPort.setLayoutData(fdPort);
      lastControl = m_wPort;
      wConnComp.setLayoutData(fdPort);
      
      // Username
      Label wlUsername = new Label(wConnComp, SWT.RIGHT);
      wlUsername.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Username.Label"));
      props.setLook(wlUsername);
      FormData fdlUsername = new FormData();
      fdlUsername.left = new FormAttachment(0, 0);
      fdlUsername.right = new FormAttachment(middle, -margin);
      fdlUsername.top = new FormAttachment(lastControl, margin);
      wlUsername.setLayoutData(fdlUsername);
      m_wUsername = new TextVar(transMeta, wConnComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wUsername);
      m_wUsername.addModifyListener(lsMod);
      FormData fdUsername = new FormData();
      fdUsername.left = new FormAttachment(middle, 0);
      fdUsername.top = new FormAttachment(lastControl, margin);
      fdUsername.right = new FormAttachment(100, 0);
      m_wUsername.setLayoutData(fdUsername);
      lastControl = m_wUsername;
      wConnComp.setLayoutData(fdUsername);
      
      // Password
      Label wlPassword = new Label(wConnComp, SWT.RIGHT);
      wlPassword.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Password.Label"));
      props.setLook(wlPassword);
      FormData fdlPassword = new FormData();
      fdlPassword.left = new FormAttachment(0, 0);
      fdlPassword.right = new FormAttachment(middle, -margin);
      fdlPassword.top = new FormAttachment(lastControl, margin);
      wlPassword.setLayoutData(fdlPassword);
      m_wPassword = new PasswordTextVar(transMeta, wConnComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wPassword);
      m_wPassword.addModifyListener(lsMod);
      FormData fdPassword = new FormData();
      fdPassword.left = new FormAttachment(middle, 0);
      fdPassword.top = new FormAttachment(lastControl, margin);
      fdPassword.right = new FormAttachment(100, 0);
      m_wPassword.setLayoutData(fdPassword);
      lastControl = m_wPassword;
      wConnComp.setLayoutData(fdPassword);
      
      wConnComp.layout();
      m_wSdbConnectionTab.setControl(wConnComp);

      // *************Input tab***********
      m_wSdbInputTab = new CTabItem(m_wTabFolder, SWT.NONE);
      m_wSdbInputTab.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.InputTab.Title")) ;
      Composite wInputComp = new Composite(m_wTabFolder, SWT.NONE);
      props.setLook(wInputComp);
      FormLayout inputLayout = new FormLayout();
      inputLayout.marginWidth = 3;
      inputLayout.marginHeight = 3;
      wInputComp.setLayout(inputLayout);
      
      // CSName
      Label wlCSName = new Label(wInputComp, SWT.RIGHT);
      wlCSName.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.CSName.Label"));
      props.setLook(wlCSName);
      FormData fdlCSName = new FormData();
      fdlCSName.left = new FormAttachment(0, 0);
      fdlCSName.right = new FormAttachment(middle, -margin);
      fdlCSName.top = new FormAttachment(0, margin);
      wlCSName.setLayoutData(fdlCSName);
      m_wCSName = new TextVar(transMeta, wInputComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wCSName);
      m_wCSName.addModifyListener(lsMod);
      FormData fdCSName = new FormData();
      fdCSName.left = new FormAttachment(middle, 0);
      fdCSName.top = new FormAttachment(0, margin);
      fdCSName.right = new FormAttachment(100, 0);
      m_wCSName.setLayoutData(fdCSName);
      lastControl = m_wCSName;
      
      // CLName
      Label wlCLName = new Label(wInputComp, SWT.RIGHT);
      wlCLName.setText(BaseMessages.getString(PKG, "SequoiaDBInput.CLName.Label"));
      props.setLook(wlCLName);
      FormData fdlCLName = new FormData();
      fdlCLName.left = new FormAttachment(0, 0);
      fdlCLName.right = new FormAttachment(middle, -margin);
      fdlCLName.top = new FormAttachment(lastControl, margin);
      wlCLName.setLayoutData(fdlCLName);
      m_wCLName = new TextVar(transMeta, wInputComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wCLName);
      m_wCLName.addModifyListener(lsMod);
      FormData fdCLName = new FormData();
      fdCLName.left = new FormAttachment(middle, 0);
      fdCLName.top = new FormAttachment(lastControl, margin);
      fdCLName.right = new FormAttachment(100, 0);
      m_wCLName.setLayoutData(fdCLName);
      lastControl = m_wCLName;
      wInputComp.setLayoutData(fdCLName);
      
      wInputComp.layout();
      m_wSdbInputTab.setControl(wInputComp);

      // *************Query tab***********
      m_wSdbQueryTab = new CTabItem(m_wTabFolder, SWT.NONE);
      m_wSdbQueryTab.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.QueryTab.Title"));
      Composite wQueryComp = new Composite(m_wTabFolder, SWT.NONE);
      props.setLook(wQueryComp);
      FormLayout queryLayout = new FormLayout();
      queryLayout.marginWidth = 3;
      queryLayout.marginHeight = 3;
      wQueryComp.setLayout(queryLayout);
      
      // Query expression(JSON)
      Label wlQuery = new Label(wQueryComp, SWT.LEFT);
      wlQuery.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.Query.Label"));
      props.setLook(wlQuery);
      FormData fdlQuery = new FormData();
      fdlQuery.left = new FormAttachment(0, 0);
      fdlQuery.right = new FormAttachment(middle, -margin);
      fdlQuery.top = new FormAttachment(0, margin);
      wlQuery.setLayoutData(fdlQuery);
      lastControl = wlQuery ;
      m_wQuery = new StyledTextComp(transMeta, wQueryComp, SWT.MULTI | SWT.LEFT
            | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
      props.setLook(m_wQuery);
      m_wQuery.addModifyListener(lsMod);
      FormData fdQuery = new FormData();
      fdQuery.left = new FormAttachment(0, 0);
      fdQuery.top = new FormAttachment(lastControl, margin);
      fdQuery.right = new FormAttachment(100, -2*margin);
      fdQuery.bottom = new FormAttachment(50, -margin);
      m_wQuery.setLayoutData(fdQuery);
      lastControl = m_wQuery;
      
      // Field Selector
      /*Label wlSelector = new Label(wQueryComp, SWT.RIGHT);
      wlSelector.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Selector.Label"));
      props.setLook(wlSelector);
      FormData fdlSelector = new FormData();
      fdlSelector.left = new FormAttachment(0, 0);
      fdlSelector.right = new FormAttachment(middle, -margin);
      fdlSelector.top = new FormAttachment(lastControl, margin);
      wlSelector.setLayoutData(fdlSelector);
      m_wSelector = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wSelector);
      m_wSelector.addModifyListener(lsMod);
      FormData fdSelector = new FormData();
      fdSelector.left = new FormAttachment(middle, 0);
      fdSelector.top = new FormAttachment(lastControl, margin);
      fdSelector.right = new FormAttachment(100, 0);
      m_wSelector.setLayoutData(fdSelector);
      lastControl = m_wSelector;
      wQueryComp.setLayoutData(fdSelector);*/
      
      // Order By
      Label wlOrderby = new Label(wQueryComp, SWT.RIGHT);
      wlOrderby.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Orderby.Label"));
      props.setLook(wlOrderby);
      FormData fdlOrderby = new FormData();
      fdlOrderby.left = new FormAttachment(0, 0);
      fdlOrderby.right = new FormAttachment(middle, -margin);
      fdlOrderby.top = new FormAttachment(lastControl, margin);
      wlOrderby.setLayoutData(fdlOrderby);
      m_wOrderby = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wOrderby);
      m_wOrderby.addModifyListener(lsMod);
      FormData fdOrderby = new FormData();
      fdOrderby.left = new FormAttachment(middle, 0);
      fdOrderby.top = new FormAttachment(lastControl, margin);
      fdOrderby.right = new FormAttachment(100, 0);
      m_wOrderby.setLayoutData(fdOrderby);
      lastControl = m_wOrderby;
      wQueryComp.setLayoutData(fdOrderby);
      
      // Skip
      Label wlSkip = new Label(wQueryComp, SWT.RIGHT);
      wlSkip.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Skip.Label"));
      props.setLook(wlSkip);
      FormData fdlSkip = new FormData();
      fdlSkip.left = new FormAttachment(0, 0);
      fdlSkip.right = new FormAttachment(middle, -margin);
      fdlSkip.top = new FormAttachment(lastControl, margin);
      wlSkip.setLayoutData(fdlSkip);
      m_wSkip = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wSkip);
      m_wSkip.addModifyListener(lsMod);
      FormData fdSkip = new FormData();
      fdSkip.left = new FormAttachment(middle, 0);
      fdSkip.top = new FormAttachment(lastControl, margin);
      fdSkip.right = new FormAttachment(100, 0);
      m_wSkip.setLayoutData(fdSkip);
      lastControl = m_wSkip;
      wQueryComp.setLayoutData(fdSkip);
      
      // Limit
      Label wlLimit = new Label(wQueryComp, SWT.RIGHT);
      wlLimit.setText(BaseMessages.getString(PKG, "SequoiaDBInput.Limit.Label"));
      props.setLook(wlLimit);
      FormData fdlLimit = new FormData();
      fdlLimit.left = new FormAttachment(0, 0);
      fdlLimit.right = new FormAttachment(middle, -margin);
      fdlLimit.top = new FormAttachment(lastControl, margin);
      wlLimit.setLayoutData(fdlLimit);
      m_wLimit = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT
            | SWT.BORDER);
      props.setLook(m_wLimit);
      m_wLimit.addModifyListener(lsMod);
      FormData fdLimit = new FormData();
      fdLimit.left = new FormAttachment(middle, 0);
      fdLimit.top = new FormAttachment(lastControl, margin);
      fdLimit.right = new FormAttachment(100, 0);
      m_wLimit.setLayoutData(fdLimit);
      lastControl = m_wLimit;
      wQueryComp.setLayoutData(fdLimit);
      
      wQueryComp.layout();
      m_wSdbQueryTab.setControl(wQueryComp);
      
      // *************Fields tab***********
      m_wSdbFieldsTab = new CTabItem(m_wTabFolder, SWT.NONE);
      m_wSdbFieldsTab.setText(BaseMessages.getString(PKG,
            "SequoiaDBInput.FieldsTab.Title"));
      Composite wFieldsComp = new Composite(m_wTabFolder, SWT.NONE);
      props.setLook(wFieldsComp);
      FormLayout fieldsLayout = new FormLayout();
      fieldsLayout.marginWidth = 3;
      fieldsLayout.marginHeight = 3;
      wFieldsComp.setLayout(fieldsLayout);

      final ColumnInfo[] colinf = new ColumnInfo[] {
          new ColumnInfo(BaseMessages.getString(PKG,
              "SequoiaDBInput.FieldsTab.FIELD_ALIAS"), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(BaseMessages.getString(PKG,
              "SequoiaDBInput.FieldsTab.FIELD_PATH"), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(BaseMessages.getString(PKG,
              "SequoiaDBInput.FieldsTab.FIELD_TYPE"), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_CCOMBO, false), };

      colinf[2].setComboValues(ValueMeta.getTypes());
      
      m_fieldsView = new TableView(transMeta, wFieldsComp, SWT.FULL_SELECTION
            | SWT.MULTI, colinf, 1, lsMod, props);
      FormData fdlFields = new FormData();
      fdlFields.top = new FormAttachment(lastControl, margin * 2);
      fdlFields.left = new FormAttachment(0, 0);
      fdlFields.right = new FormAttachment(100, 0);
      m_fieldsView.setLayoutData(fdlFields);
      
      FormData fdFields = new FormData();
      fdFields.left = new FormAttachment(0, 0);
      fdFields.top = new FormAttachment(0, 0);
      fdFields.right = new FormAttachment(100, 0);
      fdFields.bottom = new FormAttachment(100, 0);
      wFieldsComp.setLayoutData(fdFields);
      
      wFieldsComp.layout();
      m_wSdbFieldsTab.setControl(wFieldsComp);
      

      // configure the tab-folder
      FormData fdTmp = new FormData();
      fdTmp.left = new FormAttachment(0, 0);
      fdTmp.top = new FormAttachment(wStepname, margin);
      fdTmp.right = new FormAttachment(100, 0);
      fdTmp.bottom = new FormAttachment(100, -50);
      m_wTabFolder.setLayoutData(fdTmp);

      // OK and cancel buttons
      wOK = new Button(shell, SWT.PUSH);
      wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); 
      wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); 

      BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, m_wTabFolder);
      //setButtonPositions(new Button[] { wOK, wCancel }, margin, m_wTabFolder);

      // Add listeners for cancel and OK
      lsCancel = new Listener() {
         public void handleEvent(Event e) {btnCancel();}
      };
      lsOK = new Listener() {
         public void handleEvent(Event e) {btnOk();}
      };

      wCancel.addListener(SWT.Selection, lsCancel);
      wOK.addListener(SWT.Selection, lsOK);

      // default listener (for hitting "enter")
      lsDef = new SelectionAdapter() {
         public void widgetDefaultSelected(SelectionEvent e) {btnOk();}
      };
      wStepname.addSelectionListener(lsDef);
      m_wPort.addSelectionListener(lsDef);

      // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
      shell.addShellListener(new ShellAdapter() {
         public void shellClosed(ShellEvent e) {btnCancel();}
      });

      m_wTabFolder.setSelection(0);

      // Set/Restore the dialog size based on last position on screen
      // The setSize() method is inherited from BaseStepDialog
      setSize();

      // populate the dialog with the values from the meta object
      populateDialog();
      
      // restore the changed flag to original value, as the modify listeners fire during dialog population 
      m_meta.setChanged(changed);

      // open dialog and enter event loop 
      shell.open();
      while (!shell.isDisposed()) {
         if (!display.readAndDispatch())
            display.sleep();
      }

      // at this point the dialog has closed, so either btnOk() or btnCancel() have been executed
      // The "stepname" variable is inherited from BaseStepDialog
      return stepname;
	}
   
   /**
    * This helper method puts the step configuration stored in the meta object
    * and puts it into the dialog controls.
    */
   private void populateDialog() {
      m_wHostname.setText(Const.NVL(m_meta.getHostname(), ""));
      m_wPort.setText(Const.NVL(m_meta.getPort(), ""));
      m_wUsername.setText(Const.NVL(m_meta.getUserName(), ""));
      m_wPassword.setText(Const.NVL(m_meta.getPwd(), ""));
      m_wCSName.setText(Const.NVL(m_meta.getCSName(), ""));
      m_wCLName.setText(Const.NVL(m_meta.getCLName(), ""));
      m_wQuery.setText(Const.NVL(m_meta.getQuery(), ""));
      //m_wSelector.setText(Const.NVL(m_meta.getSelector(), ""));
      m_wOrderby.setText(Const.NVL(m_meta.getOrderby(), ""));
      m_wSkip.setText(Const.NVL(m_meta.getSkipStr(), ""));
      m_wLimit.setText(Const.NVL(m_meta.getLimitStr(), ""));
      wStepname.selectAll();
      setSelectedFields(m_meta.getSelectedFields());
   }
   
	/**
    * Called when the user cancels the dialog.  
    */
   private void btnCancel() {
      // The "stepname" variable will be the return value for the open() method. 
      // Setting to null to indicate that dialog was cancelled.
      stepname = null;
      // Restoring original "changed" flag on the met aobject
      m_meta.setChanged(changed);
      // close the SWT dialog window
      dispose();
   }
   
   /**
    * Called when the user confirms the dialog
    */
   private void btnOk() {
      // The "stepname" variable will be the return value for the open() method. 
      // Setting to step name from the dialog control
      if (Const.isEmpty(wStepname.getText()))
         return;
      stepname = wStepname.getText(); 
      
      getConfigure() ;
      // close the SWT dialog window
      dispose();
   }
   
   private void getConfigure()
   {
      m_meta.setHostname(m_wHostname.getText());
      m_meta.setPort(m_wPort.getText());
      m_meta.setUserName(m_wUsername.getText());
      m_meta.setPwd(m_wPassword.getText());
      m_meta.setCSName(m_wCSName.getText());
      m_meta.setCLName(m_wCLName.getText());
      m_meta.setQuery(m_wQuery.getText());
      //m_meta.setSelector(m_wSelector.getText());
      m_meta.setOrderby(m_wOrderby.getText());
      m_meta.setSkip(m_wSkip.getText());
      m_meta.setLimit(m_wLimit.getText());
      
      int numFields = m_fieldsView.nrNonEmpty();
      if (numFields > 0){
         List<SequoiaDBInputField> selectedFields = new ArrayList<SequoiaDBInputField>();
         for (int i = 0; i < numFields; i++) {
            TableItem item = m_fieldsView.getNonEmpty(i);
            SequoiaDBInputField fieldTmp = new SequoiaDBInputField();
            fieldTmp.m_fieldName = item.getText(FIRST_COL).trim();
            fieldTmp.m_path = item.getText(SECOND_COL).trim();
            if( null == fieldTmp.m_path || fieldTmp.m_path.isEmpty() ) {
               fieldTmp.m_path = fieldTmp.m_fieldName ;
            }
            fieldTmp.m_kettleType = item.getText(THIRD_COL).trim();
            selectedFields.add(fieldTmp);
         }
         m_meta.setSelectedFields(selectedFields);
      }
   }
   
   private void setSelectedFields(List<SequoiaDBInputField> fields) {
      if (null == fields) {
         return ;
      }
      
      m_fieldsView.clearAll();
      for (SequoiaDBInputField f : fields){
         TableItem item = new TableItem(m_fieldsView.table, SWT.NONE);
         
         if(!Const.isEmpty(f.m_fieldName)){
            item.setText(FIRST_COL, f.m_fieldName);
         }
         
         if(!Const.isEmpty(f.m_path)){
            item.setText(SECOND_COL, f.m_path);
         }
         
         if(!Const.isEmpty(f.m_kettleType)){
            item.setText(THIRD_COL, f.m_kettleType);
         }
      }
      
      m_fieldsView.removeEmptyRows();
      m_fieldsView.setRowNums();
      m_fieldsView.optWidth(true);
   }
}
