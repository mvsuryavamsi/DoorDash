import gspread
import pandas as pd
import os
import snowflake.connector
from datetime import datetime, timedelta,date
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
import subprocess
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import time
import ftplib
import paramiko
import time
import log_module

def sftp_files(sftp,ssh_client,ftp_path,cs,rows2,df3,ctx,gol,date_in_sheet):
    strt_time=datetime.now().strftime("%H:%M:%S")
    print("start time",strt_time)
    #files=datetime.strftime(datetime.now(),'%Y%m%d')
    file_name='Zip_codes_'+date_in_sheet+'.csv'
    Yexcel='Yes_region_zips_'+date_in_sheet+'.xlsx'
    Nexcel='No_region_zips_'+date_in_sheet+'.xlsx'
    MFITN='Yes_FINAL_FIFTEEN_'+date_in_sheet+'.csv'
    Nmain='No_FINAL_FIFTEEN_'+date_in_sheet+'.csv'
    HIGH_PAY='High_PAYOUT_ZIPS'+date_in_sheet+'.xlsx'
    MFITN='/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/'+MFITN
    Nmain='/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/'+Nmain
    path='/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/'+file_name
    files=sftp.listdir(ftp_path)
    miles=[5,10,15,30,50]
    df1=pd.DataFrame(rows2)
    Ydf=df3.loc[df3[df3.columns[-1]]=='Yes']
    latest_index = Ydf.iloc[:,[1,gol,-1]]
    Ydf2 = pd.merge(df1, latest_index, on='Submarket Name', how='inner')
    #Ydf2=Ydf2.drop_duplicates(subset=[0,4])
    Ydf2=Ydf2.drop_duplicates(subset=[Ydf2.columns[0], Ydf2.columns[4]], keep='first', inplace=False)
    #print(Ydf2.head(20))
    #Ydf2.to_csv('duplicates.csv',index=False)
    Ydf2.rename(columns = {'Zip code':'ZIPS'}, inplace = True)
    Ydf2=Ydf2[list(map(lambda x: str(x).isdigit(), Ydf2['ZIPS']))]
    Ydf2['ZIPS']=Ydf2['ZIPS'].apply(lambda x: '{0:0>5}'.format(x))
    Ydf2['ZIPS'].to_csv(path,index=False)


    zips=pd.read_csv(path,dtype={'ZIPS': 'str'})
    cs.execute("Truncate table CUSTOM_DOORDASH_ZIPS_DONOTDROP")
    ctx.commit()
    write_pandas(ctx,zips, "CUSTOM_DOORDASH_ZIPS_DONOTDROP")
    writer = pd.ExcelWriter('/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/'+Yexcel, engine='xlsxwriter')
    Ydf2.iloc[:,[0,4]].to_excel(writer, sheet_name='main_zips', index=False)
    for i in miles:
        cs.execute("select zips from CUSTOM_DOORDASH_ZIPS_DONOTDROP  union (select distinct TARGET_ZIP from ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE a,CUSTOM_DOORDASH_ZIPS_DONOTDROP b where a.source_zip=b.zips and DISTANCE <= "+str(i)+"* 1.609347218694 )")
        res=cs.fetchall()
        five = pd.DataFrame(res, columns=cs.description)
        five.to_excel(writer,sheet_name=str(i)+'_radius',index=False,header=False)
        if i==15:
            five.to_csv(MFITN,index=False,header=False)

    writer.save()
    #union_df=pd.concat([Ydf2['ZIPS'],fifteen],ignore_index=False)
    #####NO_region##########################
    df1=pd.DataFrame(rows2)
    latest_index = df3.iloc[:,[1,gol,-1]]
    Ndf2 = pd.merge(df1, latest_index, on='Submarket Name', how='left')
    Ndf2 = Ndf2[~((Ndf2.iloc[:,-1] =='Yes'))]
    Ndf2=Ndf2.drop_duplicates(subset=['Zip code'])
    Ndf2.rename(columns = {'Zip code':'ZIPS'}, inplace = True)

    Ndf2=Ndf2[list(map(lambda x: str(x).isdigit(), Ndf2['ZIPS']))]
    Ndf2['ZIPS']=Ndf2['ZIPS'].apply(lambda x: '{0:0>5}'.format(x))
    Ndf2.iloc[:,[0]].to_csv(Nmain,index=False,header=False)

        ###########Excel_Writing########################
    writer = pd.ExcelWriter('/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/'+Nexcel, engine='xlsxwriter')
    Ndf2.iloc[:,[0]].to_excel(writer, sheet_name='main_zips', index=False)
    writer.save()
    ########################################HIGH_PAY_OUT_ZIPS#############################
    HIGH = pd.ExcelWriter('/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/'+HIGH_PAY, engine='xlsxwriter')
    df1=Ydf2.iloc[:,[0,4]]
    df1.loc[:, df1.columns[-1]] = df1.loc[:, df1.columns[-1]].astype('int')
    #print(ftp_path)
    try:
        with sftp.open(ftp_path+'/Equal_CPL.txt', 'r') as f:
            b = (a.rstrip() for a in f)
            b=list(a for a in b if a)
            for i in b:
                a=i.split("|")
                Ydf=df1.loc[df1[df1.columns[-1]]==int(a[0])]
                Ydf['ZIPS'].to_csv('check2.csv',index=False)
                zips=pd.read_csv('check2.csv',dtype={'ZIPS': 'str'})
                fil='Only_$'+str(a[0])+'_'+str(a[1])+'miles'
                cs.execute("Truncate table CUSTOM_DOORDASH_ZIPS_DONOTDROP")
                ctx.commit()
                write_pandas(ctx,zips, "CUSTOM_DOORDASH_ZIPS_DONOTDROP")
                cs.execute("select zips from CUSTOM_DOORDASH_ZIPS_DONOTDROP  union (select distinct TARGET_ZIP from ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE a,CUSTOM_DOORDASH_ZIPS_DONOTDROP b where a.source_zip=b.zips and DISTANCE <="+str(a[1])+"* 1.609347218694 )")
                res=cs.fetchall()
                five = pd.DataFrame(res, columns=cs.description)
                Ydf['ZIPS'].to_excel(HIGH,sheet_name='Only_$'+str(a[0]),index=False,header=False)
                five.to_excel(HIGH,sheet_name=fil,index=False,header=False)
        f.close()
        sftp.remove(ftp_path+'/Equal_CPL.txt')
    #print(df1.head(10))
        with sftp.open(ftp_path+'/Above_CPL.txt', 'r') as f:
            b = (a.rstrip() for a in f)
            b=list(a for a in b if a)
            for i in b:
                a=i.split("|")
                Ydf=df1.loc[df1[df1.columns[-1]]>=int(a[0])]
                Ydf['ZIPS'].to_csv('check2.csv',index=False)
                zips=pd.read_csv('check2.csv',dtype={'ZIPS': 'str'})
                fil='Above_$'+str(a[0])+'_'+str(a[1])+'miles'
                cs.execute("Truncate table CUSTOM_DOORDASH_ZIPS_DONOTDROP")
                ctx.commit()
                write_pandas(ctx,zips, "CUSTOM_DOORDASH_ZIPS_DONOTDROP")
                cs.execute("select zips from CUSTOM_DOORDASH_ZIPS_DONOTDROP  union (select distinct TARGET_ZIP from ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE a,CUSTOM_DOORDASH_ZIPS_DONOTDROP b where a.source_zip=b.zips and DISTANCE <="+str(a[1])+"* 1.609347218694 )")
                res=cs.fetchall()
                five = pd.DataFrame(res, columns=cs.description)
                Ydf['ZIPS'].to_excel(HIGH,sheet_name='Above_$'+str(a[0]),index=False,header=False)
                five.to_excel(HIGH,sheet_name=fil,index=False,header=False)
        f.close()
        HIGH.save()
        sftp.remove(ftp_path+'/Above_CPL.txt')
        sftp.rmdir(ftp_path)
        sftp.close()
        ssh_client.close()
        cs.close()
        ctx.close()
        os.system("echo -e 'Hi Team,\n \n PFA for the Yes region zips. \n \n Thanks,\n DataAttribution'| mail -r 'DataAttribution' -s  'DOOR DASH  ZIPS POSTING'  -a '/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/"+Yexcel+"' -a '/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/"+Nexcel+"' -a   '/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/Zip_files/"+HIGH_PAY+"' vmarni@aptroid.com,sgollapally@aptroid.com,akhan@aptroid.com,datateam@aptroid.com,cpateam@aptroid.com")
        end_time=datetime.now().strftime("%H:%M:%S")
        time1 = datetime.strptime(strt_time, "%H:%M:%S")
        time2 = datetime.strptime(end_time, "%H:%M:%S")
        logger.info("sucess End time {0}".format(end_time))
        logger.info("Python Program Execution is completed in {} Seconds".format(time2-time1))
        logger.info("Calling shell script")
        path="/resp_cap_03/PFM_CUSTOM_SCRIPTS/ADHOC_SCRIPTS_DATATEAM/ZIPS_DATA_POSTING/zips_data_posting.sh"
        subprocess.run(['sh', path,MFITN,Nmain])
        end_time=datetime.now().strftime("%H:%M:%S")
        time1 = datetime.strptime(strt_time, "%H:%M:%S")
        time2 = datetime.strptime(end_time, "%H:%M:%S")
        logger.info("shell sucess End time {0} and shell  execution completed in {1}".format(end_time,time2-time1))
        logger.info("shell scrip execution complted")
        logger.info("sleep for 1 minute")
        time.sleep(1*60)
        main()
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        os.system("echo -e 'Hi Team,\n \n  Files not present in Directory ,Please place the files in directory "+ftp_path+" \n \n Thanks,\n DataAttribution'| mail -r 'DataAttribution' -s  'ERROR:: FILES NOT PRESENT IN  DOORDASH ZIPS  DIRECTORY'   vmarni@aptroid.com,sgollapally@aptroid.com,akhan@aptroid.com,datateam@aptroid.com,cpateam@aptroid.com")
        logger.info("sleep for 5 mins to recheck the file again")
        time.sleep(5*60)
        main()


if __name__ =="__main__":
    def main():
        try:
            global logger
            logger = log_module.setup_logging()
            SHEET_ID = '1jiP7pPc3-e6v3GNHLFAjt70aeu1RrPITmxHjj28LDew'
            SHEET_NAME = 'CPL Tolerance by Market'
            sheet2='All Submarket - Zips'
            gc =gspread.service_account('/resp_cap_03/PFM_CUSTOM_SCRIPTS/DOORDASH/ZIPS_POSTING/credentials.json')
            spreadsheet = gc.open_by_key(SHEET_ID)
            worksheet = spreadsheet.worksheet(SHEET_NAME)
            worksheet1=spreadsheet.worksheet(sheet2)
            rows2=worksheet1.get_all_records()
            rows = worksheet.get_all_values()


            df3 = pd.DataFrame(rows)
            new_header = df3.iloc[1] #grab the first row for the header
            df3 = df3[2:] #take the data less the header row
            df3.columns = new_header


            #print(gol)
            #cpl=list(new_header)[gol -1]
            les1=df3.columns
            A=list(les1)[-1].split(" ")[-1].split('/')
            y=int(datetime.strftime(datetime.now(),'%Y'))
            e = date(y,int(A[0]),int(A[1]))
            cpl=e.strftime('%B %-d')
            gol=list(new_header).index(cpl)
            #print("type of gol is :",type(gol))
            #a=e.split('-')
            #col_nam='Paid Media '+a[1]+'/'+a[2]
            logger.info("Date in Google sheet last updated {}".format(str(e).replace('-','')))
            try:
                date_in_sheet=str(e).replace('-','')
                logger.info("Etering into main function")
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect('192.200.100.122', port=22, username='Tableau', password='T@b1E@uP@ssword')
                sftp = ssh_client.open_sftp()
                ftp_path='/home/Tableau/DOORDASH_ZIPS/'
                files_in_directory = sftp.listdir(ftp_path)
                print("checking files in ftp")
                if date_in_sheet  in files_in_directory :
                    ftp_path=ftp_path+date_in_sheet
                    #time.sleep(3*60)
                    logger.info("calling sftp_files {} ".format(ftp_path))
                    try:
                        with open("/home/svczhgreen/.snowsql/rsa_key.p8", "rb") as key:
                            p_key= serialization.load_pem_private_key(
                                key.read(),
                                password='Snowfl@ke12#$'.encode(),
                                backend=default_backend()
                            )
                            pkb = p_key.private_bytes(
                                encoding=serialization.Encoding.DER,
                                format=serialization.PrivateFormat.PKCS8,
                                encryption_algorithm=serialization.NoEncryption())
                            ctx = snowflake.connector.connect(
                                user='zx_dataops_service',
                                account='zeta_hub_reader.us-east-1',
                                private_key=pkb,
                                warehouse='ZX_DATAOPS_WH',
                                database='HUBUSERS',
                                schema='ZX_DATAOPS'
                                )

                            cs = ctx.cursor()
                    except Exception as e:
                        logger.error("An error occurred: %s", str(e), exc_info=True)
                        os.system("echo -e 'Hi Team,\n \n Snow flake connection is refused in Doordash zip scritp \n \n "+str(e)+"  ,Please check ASAP  \n \n Thanks,\n DataAttribution'| mail -r 'DataAttribution' -s  'ERROR:: SNOW FLAKE CONNECTION ERROR IN  DOORDASH ZIPS '   vmarni@aptroid.com,sgollapally@aptroid.com,akhan@aptroid.com,datateam@aptroid.com")
                    sftp_files(sftp,ssh_client,ftp_path,cs,rows2,df3,ctx,gol,date_in_sheet)
                else:
                    sftp.close()
                    ssh_client.close()
                    logger.info("sleep for 15 minutes for Rechecking the files in ftp")
                    time.sleep(15*60)
                    logger.info("The time of code execution begin is : {} ".format((time.ctime())))
                    #logger.info("Script completed successfully.")
                    main()
            except Exception as e:
                logger.error("connection error with sftp : %s",str(e),exc_info=True)
                time.sleep(30)
                main()
        except Exception as e:
            print(e)
            logger.error("An error occurred: %s", str(e), exc_info=True)
            main()
    main()
    
    
    
making changes
