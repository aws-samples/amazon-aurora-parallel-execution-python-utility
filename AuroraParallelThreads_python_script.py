 # Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 # SPDX-License-Identifier: MIT-0
 #
 # Permission is hereby granted, free of charge, to any person obtaining a copy of this
 # software and associated documentation files (the "Software"), to deal in the Software
 # without restriction, including without limitation the rights to use, copy, modify,
 # merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 # permit persons to whom the Software is furnished to do so.
 #
 # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 # INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 # PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 # HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 # OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 # SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import sys, boto3, pg8000, logging, time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

QUERY="UPDATE edw_ods.ACCOUNT TGT \
SET ACCT_STS_CD = CAST(STG.status AS VARCHAR(20)), \
UPDATE_TS = CURRENT_TIMESTAMP(0) \
FROM edw_ods_stg.stg_sm_account STG \
WHERE TGT.ACCT_ID = STG.Poid_id0"

rowsPerBatch = 50000
threadPoolSize = 20
effected_rows = 0

goodqueries = []
retryqueries = []
failedqueries = []

def logStart(user,currentts,scriptname):

     b_id=getBatchId()    

     logS="INSERT INTO edw_ods.statuslog_tbl(spName, spEnv, spStart_TS, DEPT_NM, DIVISION_NM, SUBJECT_AREA_NM, spStatus, spUser, batch_id) \
     VALUES('"+scriptname+"','PROD','"+currentts+"', 'Subscriber Management', 'Consumer', 'edw_ods', 'STARTED','"+user+"',"+b_id+");"

     return logS

def logEnd(startts,endts,scriptname):

     if len(retryqueries)>0:
     
      status='FAILED'
      returncode='00001'

     else:

      status='ENDED'
      returncode='00000'

     logE="UPDATE edw_ods.statuslog_tbl \
     SET   spEnd_TS = '"+endts+"', spStatus = '"+status+"', spReturnCode = '"+returncode+"' \
     WHERE spName = '"+scriptname+"' AND spEnv = 'PROD' AND spStart_TS = '"+startts+"';"

     return logE

def confLogging(filename):

    try:

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(filename, 'w', 'utf-8')
        root_logger.addHandler(handler)

        return handler
    
    except:
       
        print('\n\tError: Could not configure logging - error reason', sys.exc_info()[0])
        print('\n\tThe error details 1: {0}'.format(sys.exc_info()[1]))
        print('\n\tThe error details 2:{0} \n'.format(sys.exc_info()[2]))

def getParam(keyname):
        # if awscli is updated with defaults, you can also pass in a profile name, or nothing for default - like just pass in 'ssm'
            #ssm = boto3.client('ssm', region_name='us-east-1', aws_access_key_id = 'mysaccesskey', aws_secret_access_key = 'mysecretaccesskey')
    try:

        my_session = boto3.session.Session()
        ssm = my_session.client('ssm', region_name='us-east-1')
        resp = ssm.get_parameter(Name=keyname,WithDecryption=True)
        return resp['Parameter']['Value']
        
    except:
        
        logstr = "\n\tError: Could not fetch the parameter values - error reason %s" % (sys.exc_info()[0])
        logging.info(logstr)

        logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
        logging.info(logstr) 

        logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
        logging.info(logstr)


def connDatabase(server,usr,pwd,db):
    
    try:
       # dateTimeObj = datetime.now()
        dbconn = pg8000.connect(host=server, user=usr, password=pwd, database=db)
       # logging.info('\nConnected to Aurora Postgres: '+ str(dateTimeObj)+'\n\n')
       #print('\nPostgres DB connection Successful\n')
        return dbconn
    except:

        logstr = "\n\t Error: Could not establish connection to postgres DB - error reason %s" % (sys.exc_info()[0])
        logging.info(logstr)

        logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
        logging.info(logstr)

        logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
        logging.info(logstr)

def logStartScript(scriptname):

    try:
       startts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
       
       server=getParam('/edo/prod/aurora/host')
       usr=getParam('/edo/prod/aurora/edw_ods/user')
       pwd=getParam('/edo/prod/aurora/edw_ods/pwd')
       db='postgres'


       dbconn=connDatabase(server,usr,pwd,db)
       
       logS=logStart(usr,str(startts),scriptname)

       with dbconn.cursor() as cur:
            cur.execute(logS)

       dbconn.commit()

       dbconn.close()
       
       return startts
    
    except:

       logstr = "\n\t Error: Could not log start of script - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)


def logEndScript(startts,scriptname):

    try:
       endts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

       server=getParam('/edo/prod/aurora/host')
       usr=getParam('/edo/prod/aurora/edw_ods/user')
       pwd=getParam('/edo/prod/aurora/edw_ods/pwd')
       db='postgres'


       dbconn=connDatabase(server,usr,pwd,db)

       logE=logEnd(str(startts),str(endts),scriptname)

       with dbconn.cursor() as cur:
            cur.execute(logE)

       dbconn.commit()

       dbconn.close()

    except:

       logstr = "\n\t Error: Could not end of script - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)


def execQuery(wherecl):

    try:
        q1ts = time.perf_counter()
        fullquery="%s %s;" %(QUERY, wherecl)
        startts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #print(startts)
        logstr = "at %s - running %s" % (str(startts), str(wherecl))
        logging.info(logstr)
       
        server=getParam('/edo/prod/aurora/host')
        usr=getParam('/edo/prod/aurora/edw_ods/user')
        pwd=getParam('/edo/prod/aurora/edw_ods/pwd')
        db='postgres'
      
        dbconn1=connDatabase(server,usr,pwd,db)
          

        with dbconn1.cursor() as cur1:
            cur1.execute(fullquery)
       
        rowcount=cur1.rowcount
        
        effected_rows =+ rowcount

        dbconn1.commit()
        
        dbconn1.close()

        endts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        q2ts = time.perf_counter()
        logstr = "Elapsed Time: %s seconds - FINISHED %s" % (str(q2ts - q1ts), str(wherecl))
        logging.info(logstr)
        goodqueries.append(wherecl)
        if (wherecl) in retryqueries:
            retryqueries.remove(wherecl)
    
    except:

        if (wherecl) in retryqueries:
            pass
        else:
            retryqueries.append(wherecl)
        
        logstr = "\n\tAn error has occurred for query execution - error reason %s" % (sys.exc_info()[0])
        logging.info(logstr)

        logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
        logging.info(logstr)

        logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
        logging.info(logstr)

def getBatchId():

    qry="select batch_id from edw_ods_stg.stg_sm_processing_batch_log;"

    try:

       server=getParam('/edo/prod/aurora/host')
       usr=getParam('/edo/prod/aurora/edw_ods/user')
       pwd=getParam('/edo/prod/aurora/edw_ods/pwd')
       db='postgres'


       dbconn=connDatabase(server,usr,pwd,db)
       cursor = dbconn.cursor()
       result = cursor.execute(qry)
       result = cursor.fetchmany(1)
       dbconn.close()
       return "".join(map(str,result[0]))

    except:

       logstr = "\n\t Failed to get Batch Id - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)

def getMaxCount():

    b_id=getBatchId()
    q="select CEIL(max(cast(seq_row_id as decimal))/"+str(rowsPerBatch)+") from edw_ods_stg.stg_sm_account;"
    
    try:
       
       server=getParam('/edo/prod/aurora/host')
       usr=getParam('/edo/prod/aurora/edw_ods/user')
       pwd=getParam('/edo/prod/aurora/edw_ods/pwd')
       db='postgres'

       dbconn=connDatabase(server,usr,pwd,db)
       cursor = dbconn.cursor()
       result = cursor.execute(q)
       result = cursor.fetchmany(1)
       dbconn.close()
       return "".join(map(str,result[0]))

    except:
       
       logstr = "\n\t Failed to determine rows per batch - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)

def retryLogic(execQuery,retryq):
	
    retryCount=0

    try:
       if len(retryqueries)>40:
        logstr = "\n No retries have been made since threshold has been breached with %s queries failed" % len(retryqueries)
        logging.info(logstr)
       else:
        retryCount=0
        while retryCount<15:
	 
         with concurrent.futures.ThreadPoolExecutor(max_workers=threadPoolSize) as executor:
          executor.map(execQuery,retryq)
         
         if len(retryqueries)==0:
          logstr = 'All queries have been successfully processed with effected rows: '+ str(effected_rows)
          logging.info(logstr)
          break
         else:
          retryCount=retryCount+1

         if retryCount>=15:
          logstr = 'Queries that failed after fifteen consecutive retries: %s' % str(retryqueries)
          logging.error(logstr)
	
		
 
	
    except:
        
       logstr = "\n\tRetry Logic execution failed - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)
	
def parallelExec(totalcount):

    try:
        runs = []
        thisIter = 0
        ts1 = time.perf_counter()
        while thisIter <= (rowsPerBatch * totalcount):
            startHere = thisIter
            endHere = thisIter + rowsPerBatch
            thisWhereCl = 'AND STG.seq_row_id BETWEEN %s AND %s' % (str(startHere), str(endHere))
            runs.append(thisWhereCl)
            thisIter = endHere + 1
                                                                    
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=threadPoolSize) as executor:
            executor.map(execQuery,runs)
                                                                                        
        # Retry
        # Log the queries that will be retried
        logstr = 'Queries to be retried: %s' % str(retryqueries)
        logging.info(logstr)

        logstr = 'Count of Retry Queries: %s' % str(len(retryqueries))
        logging.error(logstr)

        if len(retryqueries) > 0:
            retryLogic(execQuery,retryqueries)
        else:
            logstr = 'All queries have been successfully processed with effected rows: '+ str(effected_rows)
            logging.error(logstr)

                                                                                                                                            
        # Log the queries that ran successfully
        logstr = 'Queries that ran successfully: %s' % str(goodqueries)
        logging.info(logstr)

        logstr = 'Count of Successful Queries: %s' % str(len(goodqueries))
        logging.error(logstr)
                                                                                                                                                                                    
        ts2 = time.perf_counter()
        tt = ts2 - ts1
        logstr = "Time Elapsed: %s seconds" % (str(tt))
        logging.info(logstr)

    except:

       logstr = "\n\tParallel Query execution failed - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)

def main(argv):
    
    try:

        dateTimeObj = datetime.now()
        logfname='edo_sm_account_update_logfile_'+ str(dateTimeObj)+'.log'
        logfname=logfname.replace(' ','-')
        logfnamewpath='/apps/subscribermanagement/edw_ods/aurora_ods/python_logs/'+logfname

        #step 01: Update Account Table

        h=confLogging(logfnamewpath)
        b_itrs=getMaxCount()
        startts=logStartScript('python_sm_account_update')
        parallelExec(int(b_itrs))
        logEndScript(startts,'python_sm_account_update')

        if len(retryqueries) == 0:
            #step 02: Insert Account Table
            goodqueries.clear()
            effected_rows=0
            logging.getLogger().removeHandler(h)
            
            QUERY="INSERT INTO edw_ods.ACCOUNT( \
	        ACCT_ID, \
	        ACCT_GRP_ID, \
	        ACCT_NBR, \
	        ACCT_GRP_TYPE_CD, \
            ACCT_CLASS_NM, \
	        IS_TAX_TRTD_IND, \
            IS_GEO_INC_IND, \
	        IS_BUSNS_ACCT_IND, \
            ACCT_CYCLE_DMN_ID, \
            BLNG_STS_CD, \
            IS_XMPT_FROM_CLCTN_IND, \
            BAL_GRP_ID, \
	        IS_ACCT_SPNSRD_IND, \
	        LAST_BILLD_TS, \
            PAY_INFO_ID, \
	        ACCT_STS_CD, \
            SCNRIO_ID, \
            CUST_SGMNT_LIST_TXT, \
            ACCT_CRTD_DT, \
            ACCT_UPDATE_DT, \
            UPDATE_TS, \
            CREATE_TS) \
	        SELECT	STG.poid_id0 AS ACCT_ID, \
	        STG.group_obj_id0 AS ACCT_GRP_ID, \
		    CASE WHEN STG.Account_No like '%:%' THEN CAST(STG.poid_id0 AS VARCHAR(50)) ELSE STG.Account_No END AS ACCT_NBR, \
            '' AS ACCT_GRP_TYPE_CD, \
		    STG.Account_Type AS ACCT_CLASS_NM, \
		    CASE STG.Residence_Flag WHEN 0 THEN 'N' ELSE 'Y' END AS IS_TAX_TRTD_IND, \
            CASE STG.Incorporated_Flag WHEN 0 THEN 'N' ELSE 'Y' END AS IS_GEO_INC_IND, \
            CASE STG.Business_Type WHEN 0 THEN 'N' ELSE 'Y' END AS IS_BUSNS_ACCT_IND, \
            STG.actg_cycle_dom AS ACCT_CYCLE_DMN_ID, \
		    STG.billing_status AS BLNG_STS_CD, \
		    CASE STG.exempt_from_collections WHEN 0 THEN 'N' ELSE 'Y' END AS IS_XMPT_FROM_CLCTN_IND, \
		    STG.bal_grp_obj_id0 AS BAL_GRP_ID, \
		    '' AS IS_ACCT_SPNSRD_IND, \
            STG.last_bill_ts AS LAST_BILLD_TS, \
            STG.payinfo_obj_id0 AS PAY_INFO_ID, \
		    CAST(STG.status AS VARCHAR(20)) AS ACCT_STS_CD, \
		    STG.scenario_obj_id0 AS SCNRIO_ID, \
		    STG.cust_seg_list AS CUST_SGMNT_LIST_TXT, \
		    CAST(STG.created_ts AS DATE) AS ACCT_CRTD_DT, \
            CAST(STG.COMMIT_TS AS DATE) AS ACCT_UPDATE_DT, \
		    STG.mod_ts AS UPDATE_TS, \
		    CURRENT_TIMESTAMP(0) AS CREATE_TS \
	        FROM edw_ods_stg.stg_sm_account STG \
	        WHERE STG.Operation_Indicator IN ('I', 'U') \
            AND NOT EXISTS ( \
		    SELECT 1 \
            FROM edw_ods.ACCOUNT TGT \
		    WHERE TGT.ACCT_ID = STG.poid_id0)"

            dateTimeObj = datetime.now()
            logfname='edo_sm_account_insert_logfile_'+ str(dateTimeObj)+'.log'
            logfname=logfname.replace(' ','-')
            logfnamewpath='/apps/subscribermanagement/edw_ods/aurora_ods/python_logs/'+logfname
            
            confLogging(logfnamewpath)
            startts=logStartScript('python_sm_account_insert')
            parallelExec(int(b_itrs))
            logEndScript(startts,'python_sm_account_insert')




    except:
        
       logstr = "\n\t An error has been encountered during main function execution - error reason %s" % (sys.exc_info()[0])
       logging.info(logstr)

       logstr = "\n\tThe error details 1:  %s" % (sys.exc_info()[1])
       logging.info(logstr)

       logstr = "\n\tThe error details 2:  %s" % (sys.exc_info()[2])
       logging.info(logstr)
                           
#...........................
# set the main function
#...........................
if __name__ == "__main__":

    main(sys.argv[0:])
