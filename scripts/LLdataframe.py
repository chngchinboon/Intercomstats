# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 16:19:36 2016
########################################################################################################
#Current bugs
########################################################################################################
#Possible errors in overalresponse
#Possible errors in merging/updating local database
#missing issues plot has errors. reports untagged conversations, but in reality
#they're tagged. Clusters of errors usually signify errors. possible local db not updated properly.


#known flaws 28/12/2016
#tags only at top level
#multiple schools(m) with multi issue(n) will result in m*n conversations in augmenteddf
########################################################################################################
#To Do list
########################################################################################################

#current version
###### Clean up code #################
#arrange structure of code properly. 
#currently it is a damn mess.
######################################

#next version
##### DataBase #######################
#not working well :(
#build offline database for each intercom model. Done
#at every start, check for difference, merge if possible. Done
#check for match every iteration since find_all is based on last_update.  Done
#once a threshold of numerous no change matches, abort? hard to decide the threshold. Done
#perhaps average conversations a week + pid control based on previous week's number 
#build each as a separate function? Done
######################################

##### Class for conversation details #####
#Build class constructor that can accept object from intercom to turn into a dataframe
#handle both models of conversation_message and conversation_parts
##########################################

@author: Boon
"""
#%%
#from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
#from plotly.offline import download_plotlyjs, plot
#from plotly.graph_objs import *
#from plotly.graph_objs import Bar, Layout, Scatter 

#import numpy as np
import pandas as pd
#import re
#from ast import literal_eval
#import tictocgen as tt
#import xlsxwriter
#from bs4 import BeautifulSoup

from intercom.client import Client

import os.path

outputfolder=os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, 'output'))
#outputfolder=u'E:\\Boon\\intercomstats\\output'

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir)))
from configs import pat

#from intercom.client import Client
#pat=pid
intercom = Client(personal_access_token=pat)

#Intercom.app_id = pid
#Intercom.app_api_key = key

#Intercom.app_id = 
#Intercom.app_api_key = ''

import datetime
#import time
timenow=datetime.datetime.now()
timenowepoch=(timenow- datetime.datetime(1970,1,1)).total_seconds()
#datetime.datetime.fromtimestamp(int) from epoch time from intercom use localtime

#inspectiontimearray=[1,7,30,180,365] 

import plotfunc as pltf
import augfunc as af
#%%can check last updated at vs old dataframe to check for changes.
#use that to pull the conversation and append to convdf instead of rebuilding df.


convstatsf=os.path.abspath(os.path.join(outputfolder,'convstats.csv'))                 
topconvstatsf=os.path.abspath(os.path.join(outputfolder,'topconvstats.csv'))                 
#groupbyadmintatsf='groupbyadmintats.csv'
userf=os.path.abspath(os.path.join(outputfolder,'user.csv'))                 
filelist=[convstatsf,topconvstatsf,userf]
#dflist=['convdf','topconvdf','userdf']

output=True
toplot=False

#rebuild=[rebuildconvdf,rebuildtopconvdf,rebuilduser]
rebuild=[[],[],[]]
#date from intercom is in UTC.
datetimeattrlist=['created_at','first_response','first_closed','last_closed','updated_at','created_at_EOD']
datetimeattrspltlist=['created_at_Date','created_at_Time']
timedeltaattrlist=['s_to_first_response','s_to_first_closed','s_to_last_closed','s_to_last_update']
#listlist=['issue','school']

#df.Col3 = df.Col3.apply(literal_eval)
#import os.path

#load the files if rebuild is off #coerce may cause potential bugs !!!!!!!!!!!!!!
            
#load csv files loadmode 0 = just load, 1 (default) = load and update check , 2 = don't load & full rebuild
def loaddffiles(filelocation,loadmode=1):     
    datetimeattrlist=['created_at','first_response','first_closed','last_closed','updated_at']
    datetimeattrspltlist=['created_at_Date','created_at_Time']
    timedeltaattrlist=['s_to_first_response','s_to_first_closed','s_to_last_closed','s_to_last_update']
    #check if file exists. if doesn't exist need to force rebuild anyway.
    if os.path.isfile(filelocation):#update if exists          
        print ('Found file at ' +filelocation)          
    else: #rebuild if doesn't exist 
        print ('Unable to find file at ' +filelocation)                    
        loadmode=2
     
    if loadmode==0 or loadmode==1:
        outputdf=pd.read_csv(filelocation, sep='\t', encoding='utf-8',index_col=False)
        if hasattr(outputdf, u'Unnamed: 0'): del outputdf['Unnamed: 0']#might be hiding poorly merge attempts
        if hasattr(outputdf, u'Unnamed: 0.1'): del outputdf['Unnamed: 0.1']#might be hiding poorly merge attempts
        if hasattr(outputdf, 'convid'): outputdf['convid']=outputdf['convid'].astype('unicode')#loading auto changes this to int
        if hasattr(outputdf, 'assignee'): outputdf['assignee']=outputdf['assignee'].astype('unicode')#loading auto changes this to int
        for item in datetimeattrlist+datetimeattrspltlist:               
            if hasattr(outputdf, item): outputdf[item] = pd.to_datetime(outputdf[item],errors='coerce')
        for item in timedeltaattrlist:                                  
            if hasattr(outputdf, item): outputdf[item] = pd.to_timedelta(outputdf[item],errors='coerce')
          
        print ('Loaded file from ' + filelocation)          
        if loadmode==1:
             rebuild=True               
        else:
             rebuild=False               
    else:
          print ('Forcing rebuild...')          
          rebuild=True          
          outputdf=None  
          
    return outputdf, rebuild
     
#load userdf
userdf, rebuild[2]=loaddffiles(userf,1)
if userdf is not None:
    #force into types as if we loaded manually
    #del userdf['Unnamed: 0']
    userdf['anonymous']=userdf['anonymous'].astype(bool)
    userdf['unsubscribed_from_emails']=userdf['unsubscribed_from_emails'].astype(bool)        
    userdf['session_count']=userdf['session_count'].astype('int64') 
    
    #might want to combine this with top
    userdf['created_at']=pd.to_datetime(userdf['created_at'],errors='coerce')
    userdf['last_request_at']=pd.to_datetime(userdf['last_request_at'],errors='coerce')
    userdf['remote_created_at']=pd.to_datetime(userdf['remote_created_at'],errors='coerce')
    userdf['signed_up_at']=pd.to_datetime(userdf['signed_up_at'],errors='coerce')
    userdf['updated_at']=pd.to_datetime(userdf['updated_at'],errors='coerce')
    #updated_at can be used to check if needs updating
    print ('Loaded #'+str(len(userdf))+ ' users')
        
#load convdf
convdf, rebuild[0]=loaddffiles(convstatsf,1)
    #['tags'] need to be split into a list
    #assigned_to read in as float may throw an error when printing?
    #msgid read in as int64
    #notified_at read in as object
    #part_type read in as object (unicode)
    #subject read in as float64 because all NaN
    #url read in as float64 because all NaN
    
    
    
#load topconvdf
topconvdf, rebuild[1]=loaddffiles(topconvstatsf,1)
if topconvdf is not None:
    topconvdf['created_at_EOD']=pd.to_datetime(topconvdf['created_at_EOD'],errors='coerce')
    print ('Loaded #'+str(len(topconvdf))+ ' conversations')
    
    #school and tags read in as unicode
     
#%% Get admin info #small enough that can quickly get
#from intercom import Admin
admindf=pd.DataFrame([x.__dict__ for x in intercom.admins.all()]) 
print('Retrieved Admin Df from Intercom')

#split admin by country
sglist = []
with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir,'sgadminlist.txt'))) as inputfile:
    for line in inputfile:#load admin list from file. #future improvement required!
        sglist.append(line.strip())

admindf_SG=admindf[admindf.name.isin(sglist)]
admindf_MY=admindf[~admindf.name.isin(sglist)]

countrylist=['Sg','My']
admindfbycountry=[admindf_SG,admindf_MY]

#%%Count
#from intercom import Count
#AdminCount=pd.DataFrame(Count.conversation_counts_for_each_admin)
AdminCount=pd.DataFrame(intercom.counts.for_type(type='conversation', count='admin').conversation['admin'])
print('Retrieved AdminCount Df from Intercom')
#%% Get tags
#from intercom import Tag
tagdf=pd.DataFrame([x.__dict__ for x in intercom.tags.all()])

#load issue from file
issuename = []
with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir,'issuelist.txt'))) as inputfile:
    for line in inputfile:
        issuename.append(line.strip())

#group tags by issue
issuetag=tagdf[tagdf.name.isin(issuename)] 
               
#group tags by school
schooltag=tagdf[~tagdf.name.isin(issuename)]
                
print('Retrieved Issuetag and Schooltag Df from Intercom')
#%% Get Users ##########too large. need scrolling api
#loading from csv may not give recent info. need to pull from intercom for latest
#from intercom import User
userdatetimeattrlist=['created_at','last_request_at','remote_created_at','signed_up_at','updated_at']
        
def getfewusers(df, obj, num):#consider using updated_at to check if user needs to be updated!
    userdatetimeattrlist=['created_at','last_request_at','remote_created_at','signed_up_at','updated_at']
    tempuserdict=[]
    eof=False 
    for idx in xrange(num): #get num of users
        try:
             tempuserdict.append(obj[0].__dict__.copy())
                            
        except Exception, err:
             print (err)             
             eof=True
             break
    
    tempuserdf=pd.DataFrame(tempuserdict) 
    #get missing users
    if df is None:
         missinguserdf=tempuserdf.copy()
    else:
         missinguserdf=tempuserdf[~tempuserdf.id.isin(df.id)].copy()
    nummissing=len(missinguserdf)                                                          
    for attr in userdatetimeattrlist:
        missinguserdf[attr]=pd.to_datetime(missinguserdf[attr],unit='s')
     
    return missinguserdf, nummissing,eof
     
if rebuild[2]:
    print('Retrieving recent users from Intercom. This may take awhile......')    
    getmore=True    
    userobj=intercom.users.all()
    itercounter=1
    if userdf is None:
        print('Userdf missing. Rebuilding from scratch')
        retrievenumi=100    
    else:
        retrievenumi=25
    while getmore== True:
         toget=retrievenumi*2**itercounter
         missinguserdf,nummissing,eof=getfewusers(userdf,userobj,toget)
         print('Found '+str(nummissing)+'/'+str(toget)+' missing users.')
         userdf=pd.concat([userdf, missinguserdf], ignore_index=True)
         print('Updated userdf')
         itercounter+=1
         
         if nummissing>10:
              getmore=True
              print('Retrieving more to check')
         else:
              getmore=False
              print('Missing users less than 10. Exiting while loop')     
         if eof:
              getmore=False
              print ('Need to wait for scrolling api to be added by python API dev.')      
    print('Completed retrieval of user')          
    #for attr in userdatetimeattrlist:
    #    userdf[attr]=pd.to_datetime(userdf[attr],unit='s',infer_datetime_format =True)
    #print('Retrieved as many users as allowed by python-intercom API')
    

#%% Get all conversations
#from intercom import Conversation

#load issue from file
texttoremove = []
with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir,'textlist.txt'))) as inputfile:
    for line in inputfile:
        texttoremove.append(line.strip())

def getfewconv(df, convobj, num):
    if df is not None:     
         latestupdate=df.updated_at.max()-pd.Timedelta('1 days') #1 day before the max of the df  <--- depending on how often the script is run!!!!!!!! 
    else:
         latestupdate=pd.to_datetime(0)
             
    tempdictlist=[]
    eof=False 
    for idx in xrange(num): #get num of convs
        try:
             tempdictlist.append(convobj[0].__dict__.copy())                            
        except Exception, err:
             print (err)             
             eof=True
             break
    
    tempconvdf=pd.DataFrame(tempdictlist)
    #collect only those later than latestupdate    
    
    #convert to local timezone    
    tempconvdf=tempconvdf[pd.to_datetime(tempconvdf.updated_at,unit='s') > latestupdate]    
    numtoupdate=len(tempconvdf)     
    
    if numtoupdate==0:
        eof=True
    
    return tempconvdf, numtoupdate, eof

if rebuild[1]:
     if convdf is None:
         print ('Convdf is empty. Rebuilding from scratch')
     
     tomergedf=[]
     convobj=intercom.conversations.find_all()
     getmore=True          
     retrievenumi=100
     itercounter=1
     updatenumaccu=0
     while getmore== True:
         toget=retrievenumi*2**itercounter#exponentially increase number of conversations to get
         
         tomerge,numtoupdated,eof=getfewconv(topconvdf,convobj,toget)
         print('Found total '+str(numtoupdated)+'/'+str(toget)+' conversations in this set that needs updating.')
         
         if tomerge is not None:
             if itercounter==1:
                 tomergedf=tomerge.copy()
             else:
                 tomergedf=tomergedf.append(tomerge)
                  
         if numtoupdated!=0:
              getmore=True              
              print('Retrieving more to check')
         else:
              getmore=False
              print('Found no conversations to update. Exiting while loop')     
         if eof:#may be redundant (useful only if instead of a hard stop, using a threshold)
              getmore=False
              print ('Reached eof')      
         
         itercounter+=1
         updatenumaccu+=numtoupdated
         
     print('Completed retrieval.')
     print('Total of #'+str(updatenumaccu)+' conversations that needs to be updated.')
     
     totalconv=len(tomergedf)
     
     #format columns into what is required     
     tomergedf.assignee=tomergedf.assignee.apply(lambda s: s.id)
     tomergedf['adminname']=tomergedf.assignee.apply(lambda s: af.getadminname(s,admindf))
     tomergedf.user=tomergedf.user.apply(lambda s: s.id)
     tomergedf=tomergedf.rename(columns={ 'id' : 'convid'})
     del tomergedf['changed_attributes']

     for attr in userdatetimeattrlist:#may be redundant
         try: 
              tomergedf[attr]=pd.to_datetime(tomergedf[attr],unit='s')
         except KeyError:
              pass
          
     #Since userdf depends on scroll api, may be missing users from intercom          
     #scan through list of conversations to augment tomergedf with username and email.
     itercounter=1
     missinguserdf=0     
     df=[]
     
     for index, row in tomergedf.iterrows():
          try:        
               if itercounter%(int(totalconv/10))==0:#display progress counter
                      print('Processed ' + str(itercounter)+'/'+str(totalconv) + ' conversations')                     
          except ZeroDivisionError: 
               pass  
                    
          userid=row.user
          try:
              idxdf=userdf['id']==userid#count number of occurance
          except TypeError:#incase idxdf is empty
              idxdf=[0]
          if sum(idxdf)>1:#duplicate user entry. need to purge
               print('Duplicate user entry found. Please check csv/intercom')
       
          if sum(idxdf)==0:#ask intercom
               #print('Missing user '+str(userid)+'  from dataframe. Retrieving from Intercom instead')                                      
               userdetails=intercom.users.find(id=userid).__dict__.copy()    #convert to dict for storage into df
               #convert to df for merging
               userdetails=pd.DataFrame([userdetails])#need to place in list mode. possible source of error
               #convert datetime attributes to datetime objects
               for attr in userdatetimeattrlist:
                    userdetails[attr]=pd.to_datetime(userdetails[attr],unit='s')
               #append to userdf
               #print('Updated userdf')
               userdf=userdf.append(userdetails,ignore_index=True)               
               missinguserdf+=1  
               #userdetails=userdetails[['name','email']].iloc[0].tolist()
          else:#to handle multiple userid in userdf!!!!!! shouldn't be the case!!
               userdetails=userdf[userdf['id']==userid]#.iloc[0]#.to_dict()#found in df, extract values out
               #userdetails=userdetails[['name','email']].tolist()
               #userdetails=userdetails[userdetails.keys()[0]]#format may be different. possible source of errors!!!!!!!!!!!!!!!! keys used because method returns a series
          
          #df.append(dict(username=userdetails.get('name'),email=userdetails.get('email')))                                                            
          df.append(userdetails[['name','email']].iloc[0].tolist())
          itercounter+=1
          
          #df=pd.Series([dict(username=userdetails.get('name'),email=userdetails.get('email'),role=userdetails.get('role'))])
     tomergedf=tomergedf.reset_index().merge(pd.DataFrame(df,columns=['username','email']),left_index=True, right_index=True)#probably wrong here df going crazy. update:30/1/17, merging properly now     
     
     #tomergedf.update()
     
     print('Extracted all conversations to be merged')              
     print('Found #' + str(itercounter-1) + ' conversations with missing user info')
     print('Found #'+ str(missinguserdf) + ' users missing from userdf')
     print('Updated userdf')
                   
else:
     print ('Load All conversations from csv')     
     totalconv=len(topconvdf.index)     
print('Total Conversations: ' + str(totalconv))
print('Time started: '+ str(datetime.datetime.now()))               
               
 
#%% create another dataframe with all conversation parts
#tt.tic()
attrnames=['author','created_at','body','id','notified_at','part_type','assigned_to','url','attachments','subject']

conv=[]
if rebuild[0]:
     print('Retrieving full content of conversations from Intercom')
     itercounter=1     
     #consider brute force appending dicts and modify outside of loop.
     #bottleneck should still be querying from intercom
     for convid in tomergedf.convid:
          try:        
               if itercounter%(int(totalconv/10))==0:#display progress counter
                      print('Processing ' + str(itercounter)+'/'+str(totalconv) + ' conversations')                     
          except ZeroDivisionError: 
               pass          
         
          #get valuves     
          convobj=intercom.conversations.find(id=convid) #return conversation object 
          #object already has datetime attributes that are in proper format. changing to dict causes them to turn into seconds. doesn't matter can change whole column into datetime when in df form
         
          #message
          conv_message=convobj.conversation_message.__dict__.copy()    
          conv_message['convid']=convid          
          conv_message['idx_conv']=0

          #Missing
          conv_message['notified_at']=None
          conv_message['part_type']='initial'
          conv_message['assigned_to']=None

          #Modify attributes
          conv_message['created_at']=convobj.created_at.replace(tzinfo=None)
          conv_message['msgid']=conv_message['id']     
          del conv_message['id']         
          conv_message['author']=conv_message['author'].id
          conv_message['tags']=convobj.tags
          if conv_message['tags']:
               temptaglist=[]
               for numtag in conv_message['tags']:
                    temptagid=numtag.id
                    temptaglist.append(tagdf['name'][tagdf['id']==temptagid].item())                         
               #conv_message['tags']=','.join(temptaglist) #incase need to convert to strlist
               conv_message['tags']=temptaglist
          conv_message['body']=af.parsingconvtext(conv_message['body'],texttoremove)          
          
          #useless attributes
          del conv_message['changed_attributes']
          del conv_message['attachments']
          #del conv_message['body'] #<-- tracking?
          del conv_message['subject']
          del conv_message['url']
                    
          #append to final list       
          conv.append(conv_message)          

          #part
          for i,item in enumerate(convobj.conversation_parts):
               conv_part=item.__dict__.copy()               
               conv_part['convid']=convid               
               conv_part['idx_conv']=i+1
               
               #missing attributes     
               conv_part['subject']=None
               conv_part['url']=None

               #Modify attributes
               conv_part['msgid']=conv_part['id']  
               del conv_part['id']
               conv_part['created_at']=pd.to_datetime(conv_part['created_at'],unit='s')               
               conv_part['author']=conv_part['author'].id
               if conv_part['assigned_to']:
                    conv_part['assigned_to']=conv_part['assigned_to'].id
               try:
                    if conv_part['tags']:
                         temptaglist=[]
                         for numtag in conv_part['tags']:
                              temptagid=numtag.id
                              temptaglist.append(tagdf['name'][tagdf['id']==temptagid].item())                         
                         #conv_message['tags']=','.join(temptaglist)
                         conv_part['tags']=temptaglist          
               except KeyError:
                    conv_part['tags']=None
               
               conv_part['body']=af.parsingconvtext(conv_part['body'],texttoremove)               
               
               #useless attributes                            
               del conv_part['updated_at']
               del conv_part['external_id']
               del conv_part['changed_attributes']
               #del conv_part['body']
               del conv_part['attachments']
               
               #append to final list  
               conv.append(conv_part)
               #Just in case the constant request trigger api limit
               '''
               ratelimit=intercom.rate_limit_details
               if ratelimit['remaining']<25:
                      print('Current rate: %d. Sleeping for 1 min' %ratelimit['remaining'])
                      time.sleep(60)           
                      print('Resuming.....')
               '''
          itercounter+=1
          
     convdftomerge=pd.DataFrame(conv)
     print('Built convdftomerge')
     
     #convert possible datetime strings to datetime objects
     if not convdftomerge.empty:  #may not have anything to merge   
         convdftomerge['notified_at']=pd.to_datetime(convdftomerge['notified_at'],unit='s')
     
     #for attr in userdatetimeattrlist:
     #   try: 
     #        tempconvdf[attr]=pd.to_datetime(tempconvdf[attr],unit='s')
     #   except KeyError:
     #        pass
     
     #seems like can merge here
         if convdf is not None:
             #update values in common rows
             common=convdftomerge[convdftomerge.convid.isin(convdf.convid) & convdftomerge.idx_conv.isin(convdf.idx_conv)]
             convdf.update(common)
             #append missing rows
             missing=convdftomerge[~(convdftomerge.convid.isin(convdf.convid) & convdftomerge.idx_conv.isin(convdf.idx_conv))]
             convdf=convdf.append(missing)
             print('Updated convdf with convdftomerge')
         else:
             print('Convdf empty, using convdftomerge instead')
             convdf=convdftomerge
     
     #convdf['created_at']=pd.to_datetime(convdf['created_at'],unit='s')
     #split datetime into two parts so that can do comparison for time binning
     af.splitdatetime(convdftomerge,datetimeattrlist)#<--- consider bringing down during augment
     
else:
     print ('Loaded Conversations from csv')   

#tt.toc()
print('Time started: '+ str(datetime.datetime.now()))        

#%% Augment data

if rebuild[1]:
     if not tomergedf.empty:
         print('Building additional info for each conversation')     
         toaugment=tomergedf.copy()#keep original so that don't mess 
              
         #getting conversation part stats
         print('Getting Conversation part stats')
         convpartstatsdf=toaugment.convid.apply(lambda s: af.getconvpartnum(s,convdf))
         print('Conversation part stats df generated')
         
         #get tags
         print('Getting conversation school(s) and issue(s)')
         issuenschooldf=toaugment.convid.apply(lambda s: pd.Series({'numtags': len(af.gettotaltags(s,convdf)),
                                                                    'issue': af.getissue(s,convdf,issuetag),#duplicate
                                                                    'school': af.getschool(s,convdf,schooltag)#duplicate
                                                                                }))
         print('School and issue df generated')
         
         #get time info
         print('Generating key time stats')
         generateddf=toaugment.convid.apply(lambda s: af.getkeytimestats(s,convdf))
    
         print('first_response, first_closed and last_closed df generated')
         
         #some missing values need to change to be able to manipulate
         for item in datetimeattrlist+datetimeattrspltlist:               
                  #if hasattr(generateddf, item): generateddf[item] = pd.to_datetime(generateddf[item],unit='s',errors='coerce') #weird bug here. cannot coerce, will force everything to nat
                  if hasattr(generateddf, item): generateddf[item] = pd.to_datetime(generateddf[item])
         
         #get response,firstclose,lastclose timedelta 
         print('Getting timedeltas')
         tdeltadf=generateddf[['first_response', 'first_closed','last_closed']].sub(toaugment['created_at'], axis=0)
         tdeltadf.columns = ['s_to_first_response', 's_to_first_closed','s_to_last_closed']
         tudeltadf=toaugment[['updated_at']].sub(toaugment['created_at'], axis=0)
         tudeltadf.columns = ['s_to_last_update']
         print('Timedelta for first_response, first_closed, last_closed, updated_at, generated')
         
         #concat them together
         toaugment=pd.concat([toaugment,convpartstatsdf,issuenschooldf,generateddf,tdeltadf,tudeltadf], axis=1)
        
         print('Additional info for each conversation')     
         
         #change open from bool to int for easier understanding
         toaugment['open']=toaugment.open.apply(lambda s: s*1)    
         #change none to string so that can group(shifted into function)
         #toaugment['issue']=toaugment.issue.apply(lambda s: changenonetostr(s))
         #change none to string so that can group(shifted into function)
         #toaugment['school']=toaugment.school.apply(lambda s: changenonetostr(s))
         #count issues
         toaugment['numissues']=toaugment.issue.apply(lambda s: af.countissue(s))    
         
         #bintime for pivot tables
         responsebinlist=[0,1,2,3,4,365*24]
         resolvebinlist=[0,1,2,3,4,12,24,365*24]
         #toaugment['s_response_bin']=toaugment.s_to_first_response.apply(lambda s: af.bintime(s,'h',responsebinlist,0))
         responsecolumnlabels=['0-1','1-2','2-3','3-4','>4','UN']
         resolvecolumnlabels=['0-1','1-2', '2-3','3-4','4-12','12-24','>24','UN']
                           
         toaugment['s_response_bin']=pd.cut(toaugment.s_to_first_response.dt.total_seconds(),[i*3600 for i in responsebinlist],labels=responsecolumnlabels[:-1]).cat.add_categories(responsecolumnlabels[-1]).fillna(responsecolumnlabels[-1])
                  
         #can't print if type is replaced with str None
         #Have to fill nattype with none first #this screws with plotly.
         #toaugment['s_to_last_closed'] = toaugment.s_to_last_closed.apply(lambda x: x if isinstance(x, pd.tslib.Timedelta) 
         #                                      and not isinstance(x, pd.tslib.NaTType) else 'None')    
         toaugment['s_resolve_bin']=pd.cut(toaugment.s_to_last_closed.dt.total_seconds(),[i*3600 for i in resolvebinlist],labels=resolvecolumnlabels[:-1]).cat.add_categories(resolvecolumnlabels[-1]).fillna(resolvecolumnlabels[-1])
         #toaugment['s_resolve_bin']=toaugment.s_to_last_closed.apply(lambda s: af.bintime(s,'h',resolvebinlist,0))
    
         #split datetime for created_at into two parts so that can do comparison for time binning
         af.splitdatetime(toaugment,datetimeattrlist[0]) 
         #add end of created day
         toaugment['created_at_EOD']=toaugment.created_at_Date.apply(lambda s: s+pd.Timedelta('1 days')+pd.Timedelta('-1us'))
         #add first message text
         toaugment['firstmessage']=toaugment.convid.apply(lambda s: af.getfirstmessage(s,convdf))
         
         #merge the missing files!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! columns of missing and temptopconvdf different!!!!!!!!!need to check!!!! appending is screwing things up!     
         #missing is missing username(converted name from id)
         #missing role
         #extra numopened <-- last minute addition. csv file may not have
         #missing email
         #extra conversation message
         #extra changed_attributes
         
         if topconvdf is not None:
             #update values in common rows
             ##indexes of tomergedf is based on how intercom arranges the info from conversation_findall. 
             #has no meaning and relation to the topconvdf index. 
             #Have to use convid as the index instead.
             common=toaugment[toaugment.convid.isin(topconvdf.convid)]                                           
             temptopconvdf=topconvdf.set_index('convid').copy()
             temptopconvdf.update(common.set_index('convid',inplace=True))
             #temptopconvdf.reset_index(drop=True,inplace=True)
             temptopconvdf.reset_index(inplace=True)
             
             #append missing rows
             missing=toaugment[~toaugment.convid.isin(topconvdf.convid)]
             topconvdfcopy=temptopconvdf.append(missing)
             print('Updated topconvdfcopy with toaugment')
                      
         else:
             print('topconvdf empty, using toaugment instead')
             topconvdfcopy=toaugment
         
         topconvdfcopy.reset_index(drop=True,inplace=True)
     else:
         topconvdfcopy=topconvdf.copy()
         print('tomergedf empty. Skipping augmentation')
     #rename so that it doesn't conflict when pulling conversation parts
     #topconvdf=topconvdf.rename(columns={ 'id' : 'convid'})
     #convert columns with datetime strings to datetime objects
     
     ##redundant!!! removed for now
     #topconvdfcopy['updated_at']=pd.to_datetime(topconvdfcopy['updated_at'],unit='s')
     #topconvdfcopy['created_at']=pd.to_datetime(topconvdfcopy['created_at'],unit='s')
     
     #convert timedelta obj to timedelta
     #for item in timedeltaattrlist:                                  
     #         if hasattr(topconvdfcopy, item): topconvdfcopy[item] = pd.to_timedelta(topconvdfcopy[item],errors='coerce')
     
else:
     topconvdfcopy=topconvdf.copy()
     if not hasattr(topconvdfcopy,'created_at'):
         af.splitdatetime(topconvdfcopy,datetimeattrlist[0])
 
     #lists are read in as string. need to convert back so that can process. should move to common procedure when first loading in!!!!!!!!!!!!!!!!!!!!!!!!!!!
     str2listdf=topconvdfcopy.convid.apply(lambda s: pd.Series({'issue': af.getissue(s),'school': af.getschool(s)}))     #duplicate
     #cheating abit here. instead of processing string within adminconvdfcopy, getting entire data from convdf
     del topconvdfcopy['issue']
     del topconvdfcopy['school']
     topconvdfcopy=topconvdfcopy.merge(str2listdf, left_index=True, right_index=True)
     
     print('Metrics loaded from csv')                      

#%% Plotting
#%%group by tf
print('Generating plots')
print('Time started: '+ str(datetime.datetime.now()))        
#timeframe=[7,30,180,365]
timeframeend=[0,8,0,31,0,0]#[w1,w2,m1,m2,0.5y,1y]
timeframestart=[7,15,30,61,180,365]
#timeframe=[7]
#timeframedt=[timenow.date()-datetime.timedelta(dt) for dt in timeframe]
timeframestartdt=[timenow.date()-datetime.timedelta(dt) for dt in timeframestart]
timeframeenddt=[timenow.date()-datetime.timedelta(dt) for dt in timeframeend]

#for debugging
#timeinterval=[timeframestartdt[0],timeframeenddt[0]]
#ofilename='test'                

#change none to string so that can group
topconvdfcopy['issue']=topconvdfcopy.issue.apply(lambda s: af.changenonetostr(s))
topconvdfcopy['school']=topconvdfcopy.school.apply(lambda s: af.changenonetostr(s))
topconvdfcopy.adminname=topconvdfcopy.adminname.apply(lambda s: af.changenonetostr(s,'Unassigned'))
topconvdfcopy.adminname.fillna('Unassigned',inplace=True)

#make copy for plotting
topconvdfcopyutc=topconvdfcopy.copy()

#need to convert utc time to local
for item in datetimeattrlist:               
    if hasattr(topconvdfcopyutc, item): topconvdfcopyutc[item] = topconvdfcopyutc[item]+pd.Timedelta('8 hours')

#resplit to update
af.splitdatetime(topconvdfcopyutc,['created_at'])
topconvdfcopyutc['created_at_EOD']=topconvdfcopyutc.created_at_Date.apply(lambda s: s+pd.Timedelta('1 days')+pd.Timedelta('-1us'))
                 
issueschoolexpandeddf=pltf.expandtag(pltf.expandtag(topconvdfcopyutc,'issue'),'school')

Alloutdisable=False

pltsilent=[True,True]

#save folders
foldername=timenow.strftime("%Y%m%d-%H%M%S")
pathbackup=os.path.abspath(os.path.join(outputfolder,foldername))     
try: 
    os.makedirs(pathbackup)
except OSError:
    if not os.path.isdir(pathbackup):
        raise

for idx,country in enumerate(countrylist):
    #split by country
    if idx==0:
        tempexpanded=issueschoolexpandeddf[(issueschoolexpandeddf.adminname.isin(admindfbycountry[idx].name))|(issueschoolexpandeddf.adminname.isnull())] 
        temptopconvdfcopy=topconvdfcopyutc[(topconvdfcopyutc.adminname.isin(admindfbycountry[idx].name))|(topconvdfcopyutc.adminname.isnull())] 
    else:
        tempexpanded=issueschoolexpandeddf[(issueschoolexpandeddf.adminname.isin(admindfbycountry[idx].name))]                                            
        temptopconvdfcopy=topconvdfcopyutc[(topconvdfcopyutc.adminname.isin(admindfbycountry[idx].name))]                                            
    
    subfolderpath=os.path.abspath(os.path.join(outputfolder,foldername,country))     
    
    try: 
        os.makedirs(subfolderpath)
    except OSError:
        if not os.path.isdir(subfolderpath):
            raise
    
    outputstats=True
    if outputstats & ~Alloutdisable:
        pltf.agpgen(tempexpanded, [timeframestartdt[0],timeframeenddt[0]],[timeframestartdt[1],timeframeenddt[1]],os.path.abspath(os.path.join(subfolderpath,'Weeklyemail.xlsx')),responsecolumnlabels,resolvecolumnlabels)
    
    plotallconvobyadmin=True
    if plotallconvobyadmin & ~Alloutdisable:        
        pltf.allconvobyadminplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'tagsbyAdmin_1W_'+country)),silent=pltsilent[idx])
    
    plotoveralltags=True
    if plotoveralltags & ~Alloutdisable:
        pltf.overalltagplot(tempexpanded,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsformonth_'+country)),silent=pltsilent[idx])
        pltf.overalltagplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsforweek_'+country)),silent=pltsilent[idx])
    
        pltf.overalltagplot2(tempexpanded,[[timeframestartdt[3],timeframeenddt[3]],[timeframestartdt[2],timeframeenddt[2]]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsforpast2month_'+country)),silent=pltsilent[idx])
        pltf.overalltagplot2(tempexpanded,[[timeframestartdt[1],timeframeenddt[1]],[timeframestartdt[0],timeframeenddt[0]]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsforpast2week_'+country)),silent=pltsilent[idx])
                
    plotopenconvobytf=True    
    if plotopenconvobytf & ~Alloutdisable:
        pltf.openconvobytfplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'openbyday_1W_'+country)),silent=pltsilent[idx])    
        pltf.openconvobytfplot(temptopconvdfcopy,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'openbyday_1M_'+country)),silent=pltsilent[idx])
        
    plottagsbyday=True
    if plottagsbyday & ~Alloutdisable:    
        pltf.tagsbytfplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'tagsbyday_1W_'+country)),silent=pltsilent[idx])
                
    plotoverallresponsestats=True
    if plotoverallresponsestats & ~Alloutdisable:
        pltf.overallresponsestatplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'overallresponse_1W_'+country)),silent=pltsilent[idx])
        pltf.overallresponsestatplot(temptopconvdfcopy,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'overallresponse_1M_'+country)),silent=pltsilent[idx])
    
    plottagsbyschool=True
    if plottagsbyschool & ~Alloutdisable:
        try:
            pltf.tagsbyschoolplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'tagsbyschool_1W_'+country)),silent=pltsilent[idx])
            pltf.tagsbyschoolplot(tempexpanded,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'tagsbyschool_1M_'+country)),silent=pltsilent[idx])
            pltf.tagsbyschoolplot(tempexpanded,[timeframestartdt[5],timeframeenddt[5]],os.path.abspath(os.path.join(subfolderpath,'tagsbyschool_1Y_'+country)),silent=pltsilent[idx])
        except Exception, err:
            print(err)
            pass

    plotnonetags=True
    if plotnonetags & ~Alloutdisable:
        pltf.nonetagplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],'issue',os.path.abspath(os.path.join(subfolderpath,'missingissue_1W_'+country)),silent=pltsilent[idx])
        pltf.nonetagplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],'school',os.path.abspath(os.path.join(subfolderpath,'missingschool_1W_'+country)),silent=pltsilent[idx])    

#%% output to csv
## special characters are screwing with the output writing
if output:        
    convdfcopy=convdf.copy()
    #if rebuild[0]:
    #     del convdfcopy['body']
    #     del convdfcopy['subject']
    #     del convdfcopy['attachments']
              
    convdfcopy.to_csv(convstatsf, sep='\t', encoding="utf-8")
    convdfcopy.to_csv(os.path.abspath(os.path.join(outputfolder,foldername,'convstats.csv')), sep='\t', encoding="utf-8") 
    print('Written to '+convstatsf)
     
    #rearranging columns before output
    #this is screwing up the output/input!!!!!!!!!!!
    '''
    convcolumns=['adminname','convid','open','read','created_at','created_at_Date',
                      'created_at_Time','first_response','s_to_first_response','numclosed',
                      'first_closed','s_to_first_closed','last_closed','s_to_last_closed',
                      'updated_at','s_to_last_update','issue','numissues','school',
                      'numtags','nummessage','numassign','numclosed','numnote','user',
                      'username','email','role','assignee','s_response_bin',
                      's_resolve_bin']
    '''                  
    topconvdfcopyoutput=topconvdfcopy.copy()                 
    if rebuild[1]: 
         del topconvdfcopy['conversation_message']
    #topconvdfcopyoutput=topconvdfcopy[convcolumns]
    topconvdfcopyoutput.to_csv(topconvstatsf, sep='\t', encoding="utf-8")
    topconvdfcopyoutput.to_csv(os.path.abspath(os.path.join(outputfolder,foldername,'topconvstats.csv')), sep='\t', encoding="utf-8")
    print('Written to '+ topconvstatsf)
     
    if rebuild[2]:
        #need to drop duplicates. ##########potential error source
        if hasattr(userdf, 'Unnamed: 0'): del userdf['Unnamed: 0']
        userdf.drop_duplicates('id').to_csv(userf, sep='\t', encoding="utf-8")
        userdf.to_csv(userf, sep='\t', encoding="utf-8")         
        userdf.to_csv(os.path.abspath(os.path.join(outputfolder,foldername,'user.csv')), sep='\t', encoding="utf-8") 
    print('Written to '+ userf)
         

    '''
    groupedbyadminstats.to_csv(groupbyadmintatsf,sep='\t', encoding="utf-8")
    groupedbyadmindatesummary.to_csv('summary.csv',sep='\t', encoding="utf-8")
    '''