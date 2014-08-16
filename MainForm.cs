using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Configuration;
using System.Data.OracleClient;

namespace DBSync
{
    public partial class MainForm : Form
    {   
        public MainForm()
        {
            InitializeComponent();
        }

        /// <summary>
        /// This event handles the click event of the button
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button1_Click(object sender, EventArgs e)
        {
            string lApplicationId = string.Empty;
            OracleProvider lProvider = new OracleProvider();
            OracleCommand lCommand = new OracleCommand();
            string lInsertString = string.Empty;

            try
            {
                //TODO: GRANT RIGHTS

                lblStatus.Text = "Fetching Application_Id's from Source";
                
                //Fetch all records from source
                lApplicationId = FetchAllAppIDFromSource();

                if (lApplicationId == string.Empty)
                {
                    DialogResult ldr = MessageBox.Show("No SUBMIT_DATE found in SOURCE Schema.\nCopy all the data from Source to destination ?", "Error", MessageBoxButtons.YesNo);

                    if (ldr == DialogResult.Yes)
                    {
                        lApplicationId = GetAllApplicationId();
                    }
                    else
                    { 
                        // do nothing
                        return;
                    }

                }

                //Open connection and begin transaction
                lCommand = lProvider.OpenConnectionAndBeginTransaction();

                lblStatus.Text = "Copying data into T_APPLICATION Target Table.";

                //Get T_APPLICATION insert query
                lInsertString = GetQueryForTarget(TableName.T_APPLICATION, lApplicationId);

                if (lProvider.ExecuteNonQueryTrans(lCommand, lInsertString) > 0)
                {
                    lblStatus.Text = "Copying data into T_APPLICATION_ACTIVITIES Target Table.";

                    //Get T_APPLICATION_ACTIVITIES insert query
                    lInsertString = GetQueryForTarget(TableName.T_APPLICATION_ACTIVITIES, lApplicationId);

                    lProvider.ExecuteNonQueryTrans(lCommand, lInsertString);

                    lblStatus.Text = "Copying data into T_APPLICATION_FINANCE Target Table.";

                    //Get T_APPLICATION_FINANCE insert query
                    lInsertString = GetQueryForTarget(TableName.T_APPLICATION_FINANCE, lApplicationId);

                    lProvider.ExecuteNonQueryTrans(lCommand, lInsertString);

                    lblStatus.Text = "Copying data into T_BOARD_DIRECTORS Target Table.";

                    //Get T_BOARD_DIRECTORS insert query
                    lInsertString = GetQueryForTarget(TableName.T_BOARD_DIRECTORS, lApplicationId);

                    lProvider.ExecuteNonQueryTrans(lCommand, lInsertString);

                    //Commit the transaction
                    lProvider.CommitTransaction(lCommand);

                    lblStatus.Text = "Success!";
                    MessageBox.Show("Data Copied to Target Schema!","Success");

                }
                else
                {
                    lProvider.RollBackTransaction(lCommand);
                }
                
                
            }
            catch (Exception ex)
            {
                lProvider.RollBackTransaction(lCommand);
                MessageBox.Show("EXCEPTION: " + ex.Message);   
            }
        }

        /// <summary>
        /// This function fetches all the application id's from source based on a condition
        /// </summary>
        /// <returns>comma separated application_id</returns>
        private string FetchAllAppIDFromSource()
        {
            try
            {
                string lSourceConnString = ReadConfigFile("SourceConnectionString");
                string lQuery = string.Empty;
                string lApplicationId = "'";

                lQuery = " SELECT APPLICATION_ID ";
                lQuery = lQuery + "    FROM " + ReadConfigFile("SourceUser") + ".T_EXT_APPLICATION ";
                lQuery = lQuery + "   WHERE SUBMIT_DATE IS NOT NULL ";
                lQuery = lQuery + "         AND SUBMIT_DATE > ";
                lQuery = lQuery + "                (SELECT MAX (SUBMIT_DATE) FROM " + ReadConfigFile("DestUser") + ".T_APPLICATION) ";
                lQuery = lQuery + "ORDER BY SUBMIT_DATE ASC ";

                OracleProvider lProvider = new OracleProvider();
                DataTable lRecords = lProvider.FetchDataFromDB(lQuery, lSourceConnString);

                foreach (DataRow ldr in lRecords.Rows)
                {
                    lApplicationId = lApplicationId + ldr["APPLICATION_ID"].ToString() + "','";
                }

                if (lApplicationId.Length > 1)
                {
                    lApplicationId = lApplicationId.Remove(lApplicationId.Length - 2, 2);
                }
                else
                {
                    lApplicationId = string.Empty;
                }

                return lApplicationId;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Exception: " + ex.Message);
                return string.Empty;
            }            

        }

        /// <summary>
        /// This function reads the app.config. 
        /// </summary>
        /// <param name="pSettingName">Key name for the setting</param>
        /// <returns></returns>
        private string ReadConfigFile(string pSettingName)
        {
            return ConfigurationManager.AppSettings[pSettingName];
        }

        /// <summary>
        /// This function returns query for bulk insert in all the tables
        /// </summary>
        /// <param name="pTbName"></param>
        /// <param name="pApplicationId"></param>
        /// <returns></returns>
        private string GetQueryForTarget(TableName pTbName, string pApplicationId)
        {
            string SQL = string.Empty;

            if (pTbName == TableName.T_APPLICATION)
            {
                SQL = " INSERT INTO " + ReadConfigFile("DestUser") + ".T_APPLICATION ";
                SQL = SQL + "   SELECT * ";
                SQL = SQL + "     FROM " + ReadConfigFile("SourceUser") + ".T_EXT_APPLICATION s ";
                SQL = SQL + "    WHERE s.APPLICATION_ID IN (" + pApplicationId + ") ";
            }
            else if (pTbName == TableName.T_APPLICATION_ACTIVITIES)
            {
                SQL = " INSERT INTO " + ReadConfigFile("DestUser") + ".T_APPLICATION_ACTIVITIES ";
                SQL = SQL + "   SELECT * ";
                SQL = SQL + "     FROM " + ReadConfigFile("SourceUser") + ".T_EXT_APPLICATION_ACTIVITIES  s ";
                SQL = SQL + "    WHERE s.APPLICATION_ID IN (" + pApplicationId + ") ";
            }
            else if (pTbName == TableName.T_APPLICATION_FINANCE)
            {
                SQL = " INSERT INTO " + ReadConfigFile("DestUser") + ".T_APPLICATION_FINANCE ";
                SQL = SQL + "   SELECT * ";
                SQL = SQL + "     FROM " + ReadConfigFile("SourceUser") + ".T_EXT_APPLICATION_FINANCE   s ";
                SQL = SQL + "    WHERE s.APPLICATION_ID IN (" + pApplicationId + ") ";
            }
            else if (pTbName == TableName.T_BOARD_DIRECTORS)
            {
                SQL = " INSERT INTO " + ReadConfigFile("DestUser") + ".T_BOARD_DIRECTORS ";
                SQL = SQL + "   SELECT * ";
                SQL = SQL + "     FROM " + ReadConfigFile("SourceUser") + ".T_EXT_BOARD_DIRECTORS    s ";
                SQL = SQL + "    WHERE s.APPLICATION_ID IN (" + pApplicationId + ") ";
            }

            return SQL;            
        }

        /// <summary>
        /// This function returns all the application ids from the source table
        /// </summary>
        /// <returns></returns>
        private string GetAllApplicationId()
        { 
            try
            {
                string lSourceConnString = ReadConfigFile("SourceConnectionString");
                string lQuery = string.Empty;
                string lApplicationId = "'";

                lQuery = " SELECT APPLICATION_ID FROM SOURCEDB.T_EXT_APPLICATION ORDER BY APPLICATION_ID ";                

                OracleProvider lProvider = new OracleProvider();
                DataTable lRecords = lProvider.FetchDataFromDB(lQuery, lSourceConnString);

                foreach (DataRow ldr in lRecords.Rows)
                {
                    lApplicationId = lApplicationId + ldr["APPLICATION_ID"].ToString() + "','";
                }

                if (lApplicationId.Length > 1)
                {
                    lApplicationId = lApplicationId.Remove(lApplicationId.Length - 2, 2);
                }
                else
                {
                    lApplicationId = string.Empty;
                }

                return lApplicationId;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Exception: " + ex.Message);
                return string.Empty;
            }            

        }

        public enum TableName
        {
            T_APPLICATION,
            T_APPLICATION_ACTIVITIES ,
            T_APPLICATION_FINANCE,
            T_BOARD_DIRECTORS
        }
    }
}
