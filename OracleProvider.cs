using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.OracleClient;
using System.Configuration;
using System.Data;
using System.Collections;

namespace DBSync
{
    public class OracleProvider
    {
        public DataTable FetchDataFromDB(string pQuery,string pConnectionString)
        {
            OracleConnection myConnection;
            OracleCommand myCommand;
            OracleDataReader dr;
            System.Data.DataTable dt = new System.Data.DataTable();

            myConnection = new OracleConnection(pConnectionString);

            try
            {
                myConnection.Open();
                myCommand = new OracleCommand(pQuery, myConnection);
                dr = myCommand.ExecuteReader();

                dt.Load(dr);

                return dt;
            }
            catch (Exception ex)
            {
                return null;
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
        /// This function opens a connecion, assignes transaction to the command object and returns the command object
        /// </summary>
        /// <returns></returns>
        public OracleCommand OpenConnectionAndBeginTransaction()
        {
            OracleConnection myConnection;
            OracleCommand myCommand;
            OracleTransaction lor;

            myConnection = new OracleConnection(ReadConfigFile("DestConnectionString"));

            myConnection.Open();
            myCommand = myConnection.CreateCommand();

            lor = myConnection.BeginTransaction(IsolationLevel.ReadCommitted);
            myCommand.Transaction = lor;

            return myCommand;
        }

        /// <summary>
        /// This fuction takes a command object and commits the transaction
        /// </summary>
        /// <param name="pCommand"></param>
        public void CommitTransaction(OracleCommand pCommand)
        {
            if (pCommand != null && pCommand.Transaction != null)
            {
                pCommand.Transaction.Commit();
            }
        }

        /// <summary>
        /// This function takes a transaction and roll backs it
        /// </summary>
        /// <param name="pCommand"></param>
        public void RollBackTransaction(OracleCommand pCommand)
        {
            if (pCommand != null && pCommand.Transaction != null)
            {
                pCommand.Transaction.Rollback();
            }
        }

        /// <summary>
        /// This fuction takes command, query and parameters and executes the query
        /// </summary>
        /// <param name="pCommand"></param>
        /// <param name="pQuery"></param>
        /// <param name="pParamCollection"></param>
        /// <returns></returns>
        public int ExecuteNonQueryTrans(OracleCommand pCommand, string pQuery)
        {
            if (pCommand.Connection != null)
            {                
                pCommand.CommandText = pQuery;
                return pCommand.ExecuteNonQuery();
                
            }
            else
            {
                return -2;
            }
        }        
    }
}
