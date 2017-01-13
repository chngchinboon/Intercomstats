# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 16:19:36 2016
########################################################################################################
#Current bugs
########################################################################################################
#Current version working for pure rebuild or pure reload. unsure of bugs if seperate sections
#are loaded differently.

#buglist 20/12/2016
#open convo not reflected 
#nic - 20 dec
#trang - 15dec
#bug for trang tracked to conversations that have been closed/opened multiple times. 
#Since algo uses first_closed to check, currently open conversations will be lose to the bin it was first_closed
#huh



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

###### Intercom data retrieval method #####
#Currently iterating through admin list to obtaining conversations from each admin.
#May be missing out on conversations that have no admin assigned. 
#Evident from number of rows of adminconvdf vs number for all in Intercom.
#Probably should pull from Conversations.find_all() instead and build own list.
#Currently don't know how to identify admin owner of conversation. only know who is last assigned
########################################### 

#next version
##### DataBase #######################
#build offline database for each intercom model
#at every start, check for difference, merge if possible.
#check for match every iteration since find_all is based on last_update. 
#once a threshold of numerous no change matches, abort? hard to decide the threshold.
#perhaps average conversations a week + pid control based on previous week's number
#build each as a separate function? Done
######################################

##### Class for conversation details #####
#Build class constructor that can accept object from intercom to turn into a dataframe
#handle both models of conversation_message and conversation_parts
##########################################

##### Harmonize/standardize output formats ###### 
#Current load from intercom -> output and load from csv is differnt.
#Problem is due to pandas' to_csv is screwing over alot of the formatting
#reading in via read_csv doesn't return the exact format as when output.
#Currently adminconvdf is kind of fixed. convdf is not.
########################################################################################################

##### Reduce input parameters for plotting ###### 
#Currently specifying input pivot table from list, outputname, timeinterval(str), number of conversation
#may want to pull in entire row of list instead.
########################################################################################################

##### Sort by conversations by admin for missing tags, give conversation details in annotation in order for people to trace #####################


@author: Boon
"""
#%%
#from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
from plotly.offline import download_plotlyjs, plot
#from plotly.graph_objs import *
from plotly.graph_objs import Bar, Layout, Scatter 

import numpy as np
import pandas as pd
from ast import literal_eval
import tictocgen as tt

from intercom import Intercom

import os.path

outputfolder=os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, 'output'))
#outputfolder=u'E:\\Boon\\intercomstats\\output'

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir)))
from configs import pid,key

#from intercom.client import Client
#pat=pid
#intercom = Client(personal_access_token=pat)


Intercom.app_id = pid
Intercom.app_api_key = key


import datetime
import time
timenow=datetime.datetime.now()
timenowepoch=(timenow- datetime.datetime(1970,1,1)).total_seconds()
#datetime.datetime.fromtimestamp(int) from epoch time from intercom use localtime

#inspectiontimearray=[1,7,30,180,365] 

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
datetimeattrlist=['created_at','first_response','first_closed','last_closed','updated_at']
datetimeattrspltlist=['created_at_Date','created_at_Time']
timedeltaattrlist=['s_to_first_response','s_to_first_closed','s_to_last_closed','s_to_last_update']
#listlist=['issue','school']

#df.Col3 = df.Col3.apply(literal_eval)
#import os.path

#load the files if rebuild is off #coerce may cause potential bugs !!!!!!!!!!!!!!
'''
for i,nfile in enumerate(filelist):
    if os.path.isfile(nfile):
        if not rebuild[i]:            
            exec("%s = pd.read_csv((\'%s\'), sep='\t', encoding='utf-8')" %(dflist[i],filelist[i]))
            exec("if hasattr(%s, 'Unnamed: 0'): del %s['Unnamed: 0']" %(dflist[i],dflist[i]))
            exec("if hasattr(%s, 'convid'): %s['convid']=%s['convid'].astype('unicode')" %(dflist[i],dflist[i],dflist[i]))
            exec("if hasattr(%s, 'assignee'): %s['assignee']=%s['assignee'].astype('unicode')" %(dflist[i],dflist[i],dflist[i]))
            for item in datetimeattrlist+datetimeattrspltlist:               
                exec("if hasattr(%s, item): %s[item] = pd.to_datetime(%s[item],errors='coerce')"  %(dflist[i],dflist[i],dflist[i]))                    
            for item in timedeltaattrlist:                                  
                exec("if hasattr(%s, item): %s[item] = pd.to_timedelta(%s[item],errors='coerce')" %(dflist[i],dflist[i],dflist[i]))                         
            #for item in listlist:#trying to curnnot sure how to do
                #exec("if hasattr(%s, item): %s[item] = %s[item].apply(literal_eval)" %(dflist[i],dflist[i],dflist[i]))            
            print('Loaded ' + dflist[i] + ' from ' + filelist[i])
'''
            
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
        if hasattr(outputdf, 'Unnamed: 0'): del outputdf['Unnamed: 0']
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
from intercom import Admin
admindf=pd.DataFrame([x.__dict__ for x in Admin.all()]) 
print('Retrieved Admin Df from Intercom')

#%%Count
from intercom import Count
AdminCount=pd.DataFrame(Count.conversation_counts_for_each_admin)
print('Retrieved AdminCount Df from Intercom')
#%% Get tags
from intercom import Tag
tagdf=pd.DataFrame([x.__dict__ for x in Tag.all()])

#group tags by issue
issuename=['Admin','Apps','Attendance','Bug','Bulletins','Check In/Out',
           'Checklist','Contact Sales Support','Feedback','Fees',
           'Forward to Malaysia Team','Forward to School','Integration Issue',
           'Internal:SPAM','LFR','Login Help','Logs','Moments','Notifications',
           'Other Issue Type','Portfolio','Promotion','Wrong Parent Particulars',
           'Weekly Digest','Change of Particulars','User Guide', 'Duplicate',
           'Wrong Recipient','Security Alert (Google)','General Enquiry','Spam',
           'User Guide','Mailgun','Yahoo Mail Block', 'Yahoo Mail Throttle/Block']

issuetag=tagdf[tagdf.name.isin(issuename)] 
               
#group tags by school
schooltag=tagdf[~tagdf.name.isin(issuename)]
                
print('Retrieved Issuetag and Schooltag Df from Intercom')
#%% Get Users ##########too large. need scrolling api
#loading from csv may not give recent info. need to pull from intercom for latest
from intercom import User
userdatetimeattrlist=['created_at','last_request_at','remote_created_at','signed_up_at','updated_at']
'''
if rebuild[2]:
    print('Retrieving users from Intercom. This will take awhile......')
    userdict=[]
    itercounter=1
    try:
        for item in User.all():
            try:        
                if itercounter%(250)==0:#display progress counter
                    print('Processed ' + str(itercounter) + ' users')                     
            except ZeroDivisionError: 
                pass
                
            userdict.append(item.__dict__.copy())
            itercounter+=1
    
    except Exception, err:
        print (err)
        print ('Need to wait for scrolling api to be added by python API dev.')
        
    userdf=pd.DataFrame(userdict) 
    
    for attr in userdatetimeattrlist:
        userdf[attr]=pd.to_datetime(userdf[attr],unit='s')
    print('Retrieved as many users as allowed by python-intercom API')
'''    
        
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
    userobj=User.all()
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
    
    

#%% funky function
#split datetime into date and time
def splitdatetime(dataframe,attrlist):
     if type(attrlist)==str:
         attrlist=[attrlist]     

     for item in attrlist:
          if hasattr(dataframe, item):
               #try:
               temp = pd.DatetimeIndex(dataframe[item])                   
               #except TypeError:               
               #    print(item)                   
               #    print(type(item))
                   #pass
               dataframe[item+'_Date'] = temp.date
               dataframe[item+'_Date'] = pd.to_datetime(dataframe[item+'_Date'])
               dataframe[item+'_Time'] = temp.time               
               #dataframe[item+'_Time'] = pd.to_datetime(dataframe[item+'_Time'])
               #del dataframe[item]
#need to return new dataframe to be merged back. current implementation is terrible               
#%%
def getadminname(s,admindf):
     extractednamelist=admindf[admindf.id==s].name.values
     if extractednamelist:
          adminname=extractednamelist[0]
     else:
          adminname=None
     return adminname             
     
#%% Get all conversations
from intercom import Conversation

def getfewconv(df, convobj, num):
    if df is not None:     
         latestupdate=df.updated_at.max() #latest update available in df.
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
    #collect only those later than the df
    tempconvdf=tempconvdf[pd.to_datetime(tempconvdf.updated_at,unit='s') > latestupdate]    
    numtoupdate=len(tempconvdf)     
    
    if numtoupdate==0:
        eof=True
    
    return tempconvdf, numtoupdate, eof

if rebuild[1]:
     if convdf is None:
         print ('Convdf is empty. Rebuilding from scratch')
     
     tomergedf=[]
     convobj=Conversation.find_all()
     getmore=True          
     retrievenumi=50
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
     tomergedf['adminname']=tomergedf.assignee.apply(lambda s: getadminname(s,admindf))
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
               userdetails=User.find(id=userid).__dict__.copy()    #convert to dict for storage into df
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
     tomergedf=tomergedf.merge(pd.DataFrame(df,columns=['username','email']),left_index=True, right_index=True)#probably wrong here df going crazy     
     
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
          convobj=Conversation.find(id=convid) #return conversation object 
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
          conv_message['created_at']=convobj.created_at
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
          
          #useless attributes
          del conv_message['changed_attributes']
          del conv_message['attachments']
          del conv_message['body'] #<-- tracking?
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
                                              
               #useless attributes                            
               del conv_part['updated_at']
               del conv_part['external_id']
               del conv_part['changed_attributes']
               del conv_part['body']
               del conv_part['attachments']
               
               #append to final list  
               conv.append(conv_part)
               #Just in case the constant request trigger api limit
               if Intercom.rate_limit_details['remaining']<25:
                      print('Current rate: %d. Sleeping for 1 min' %Intercom.rate_limit_details['remaining'])
                      time.sleep(60)           
                      print('Resuming.....')
                      
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
     splitdatetime(convdftomerge,datetimeattrlist)#<--- consider bringing down during augment
     
else:
     print ('Loaded Conversations from csv')   

#tt.toc()

#%% Calculate values to update adminconvdf
# Get First response, first closed, last closed
def getkeytimestats(s,refconvdf):
    df=refconvdf[refconvdf.convid==s]
    
    #first response
    firstrsp=df[df.idx_conv==1]
    if firstrsp.empty:
        firstrsp=None
    else: 
        firstrsp=firstrsp.created_at.iloc[0]
    
    #closed part types    
    clsparts=df[df.part_type=='close']
    
    #first closed
    firstcls=clsparts.head(1)
    if firstcls.empty:
        firstcls=None    
    else: 
        firstcls=firstcls.created_at.iloc[0]
    
    #clast closed
    lastcls=clsparts.tail(1)
    if lastcls.empty:
        lastcls=None
    else: 
        lastcls=lastcls.created_at.iloc[0]
    return pd.Series(dict(first_response=firstrsp,first_closed=firstcls,last_closed=lastcls))
    
'''
#Find response time 
def getfirstresponse(s,refconvdf):    
    firstrsp=refconvdf[(refconvdf.convid==s) & (refconvdf.idx_conv==1)]
    if not firstrsp.empty:
        return firstrsp.created_at.values[0]
    else: 
        return None

#Find first closed time
def getfirstclosed(s,refconvdf):
    firstcls=refconvdf[(refconvdf.convid==s) & (refconvdf.part_type=='close')]
    if not firstcls.empty:                  
         return firstcls.head(1).created_at.values[0]
    else: 
         return None
#last closed time          
def getlastclosed(s,refconvdf):
    lastcls=refconvdf[(refconvdf.convid==s) & (refconvdf.part_type=='close')]
    if not lastcls.empty:                      
         return lastcls.tail(1).created_at.values[0]
    else: 
         return None        
'''        
def getconvpartnum(s,refconvdf):    
    #create empty series             
    numcount=pd.Series(dict(close=0,comment=0,assignment=0,note=0,open=0))
    #update using retrieved stats
    df=refconvdf[(refconvdf.convid==s)]
    numcount.update(df.part_type.value_counts())
    #Force name change
    numcount.rename({ 'close' : 'numclosed','comment':'nummessage','assignment':'numassign','note':'numnote','open':'numopened'},inplace=True)    
    return numcount
        
def gettotaltags(s,refconvdf):         
     taglist=[]     
     for ptag in refconvdf[(refconvdf.convid==s)].tags.values:
          if type(ptag)==str or type(ptag)==unicode:
               ptag=ptag[1:-1].split(', ')        #possible source of error.. added space to cater for reading in csv.       
          if ptag:
               try: 
                    for ele in ptag:
                         taglist.append(ele)     
               except TypeError:
                    pass
     return taglist
     
             
def getschool(s,refconvdf):
     #some conversation might be forward by admin through email and thus not suitable to check user for details.
     #check if empty     
     taglist=gettotaltags(s,refconvdf)     
     schoolname=list(set(schooltag.name.values).intersection(taglist))
     if not schoolname:
          return 'None'
     else:
          return schoolname
                    
          '''
          userid=convdf[(convdf.convid==s)& (convdf.idx_conv==0)].author.values[0]
          print('Missing schoolname - trying to get')
          try: 
               schoolname=User.find(id=userid).custom_attributes['School Name']
               return schoolname
          except KeyError:
               return None
          '''     
     '''                                           
     if numtags==0:# check user if empty tag list
          userid=convdf[(convdf.convid==s)& (convdf.idx_conv==0)].author.values[0]
          return User.find(id=userid).custom_attributes['School Name']
     else:
          schoolname=list(set(schooltag.name.values).intersection(taglist))
          return schoolname
     '''
def getissue(s,refconvdf):     
     #check if empty
     taglist=gettotaltags(s,refconvdf)
     issuename=list(set(issuetag.name.values).intersection(taglist))     
     if not issuename:
          return 'None'
     else:
          return issuename

def countissue(s):    #assuming issues are in list
    if s:
        if type(s)==str or type(s)==float:
            return 0
        else:
            return len(s)
    else:
        return 0              
        
def changenonetostr(s):    #assuming issues are in list
    if not s:
        return 'None'
    else:
        return s
        
def changenonetoNone(s):    #
    if s=='None':
        return None
    else:
        return s
        
def changenonetotimedeltazero(s):    #grrr
    if s=='None' or s is None:
        return pd.Timedelta(0)
    else:
        return s
        
def changenattotimedeltazero(s):    #assuming issues are in list
    if type(s)==pd.tslib.NaTType:
        return pd.Timedelta(0)
    else:
        return s

def bintime(s,tunit,timebin,nanval):    
    #timeunits=s / np.timedelta64(1, unit)
    for i in timebin[0:-1]:
        if s == 'None' or type(s)==pd.tslib.NaTType:
            binval=nanval
            break
        if s <= np.timedelta64(i, tunit):            
            binval=i
            break
        else:             
            binval= timebin[-1]
    return binval        

def recogtf(tf,timebin):#for printing timeframe in context
    timeframe=[7,30,180,365]
    tfstr=['Week','Month','6 Months','Year']    
    binout=bintime(pd.Timedelta(tf),'D',timebin,0)
    binoutidx=[i for i,x in enumerate(timeframe) if x==binout]    
    return tfstr[binoutidx[0]]

if rebuild[1]:
     if not tomergedf.empty:
         print('Building additional info for each conversation')     
         toaugment=tomergedf.copy()#keep original so that don't mess 
              
         #getting conversation part stats
         print('Getting Conversation part stats')
         convpartstatsdf=toaugment.convid.apply(lambda s: getconvpartnum(s,convdf))
         print('Conversation part stats df generated')
         
         #get tags
         print('Getting conversation school(s) and issue(s)')
         issuenschooldf=toaugment.convid.apply(lambda s: pd.Series({'numtags': len(gettotaltags(s,convdf)),
                                                                    'issue': getissue(s,convdf),#duplicate
                                                                    'school': getschool(s,convdf)#duplicate
                                                                                }))
         print('School and issue df generated')
         
         #get time info
         print('Generating key time stats')
         generateddf=toaugment.convid.apply(lambda s: getkeytimestats(s,convdf))
    
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
         toaugment['numissues']=toaugment.issue.apply(lambda s: countissue(s))    
         
         #bintime for pivot tables
         toaugment['s_response_bin']=toaugment.s_to_first_response.apply(lambda s: bintime(s,'h',[1,2,3,4],0))
         #can't print if type is replaced with str None
         #Have to fill nattype with none first #this screws with plotly.
         #toaugment['s_to_last_closed'] = toaugment.s_to_last_closed.apply(lambda x: x if isinstance(x, pd.tslib.Timedelta) 
         #                                      and not isinstance(x, pd.tslib.NaTType) else 'None')    
         toaugment['s_resolve_bin']=toaugment.s_to_last_closed.apply(lambda s: bintime(s,'h',[1,2,3,4],0))
    
         #split datetime for created_at into two parts so that can do comparison for time binning
         splitdatetime(toaugment,datetimeattrlist[0]) 
         #add end of created day
         toaugment['created_at_EOD']=toaugment.created_at_Date.apply(lambda s: s+pd.Timedelta('1 days')+pd.Timedelta('-1us'))
         
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
         splitdatetime(topconvdfcopy,datetimeattrlist[0])
 
     #lists are read in as string. need to convert back so that can process. should move to common procedure when first loading in!!!!!!!!!!!!!!!!!!!!!!!!!!!
     str2listdf=topconvdfcopy.convid.apply(lambda s: pd.Series({'issue': getissue(s),'school': getschool(s)}))     #duplicate
     #cheating abit here. instead of processing string within adminconvdfcopy, getting entire data from convdf
     del topconvdfcopy['issue']
     del topconvdfcopy['school']
     topconvdfcopy=topconvdfcopy.merge(str2listdf, left_index=True, right_index=True)
     
     print('Metrics loaded from csv')                      

#%% 
def slicebytimeinterval(df,timeinterval,column='created_at_Date'):
    if timeinterval[0]>timeinterval[1]:
        print('Warning: timestart > timeend') 
    sliceddf=df[(df[column] > pd.to_datetime(timeinterval[0])) & (df[column] <= pd.to_datetime(timeinterval[1]))]
    return sliceddf


def expandtag(df,tagtype): #need to double check to see if truly duplicating properly--------------------------------------------------------
    #use nested expandtag(expandtag(df,tagtype),tagtype) for both issue and school
    if tagtype=='issue':
        emptyrow=df[df['numissues']==0]#collect rows with issues equal to 0    
        filledrow=df[df['numissues']>0]#collect rows with issues greater than 1
    elif tagtype=='school':
        emptyrow=df[df['school']=='None']#collect rows with schools with none    
        filledrow=df[df['school']!='None']#collect rows with schools 
    
    #Build new df 
    newdf=[]
    for index, row in filledrow.iterrows():                   
        if type(row[tagtype])==unicode:
            row[tagtype]=row[tagtype][1:-1].split(', ')
        for multitag in row[tagtype]:            
            temprow=row.copy()#duplicate row
            temprow[tagtype]=multitag#replace multi issue of duplicated row with single issue
            newdf.append(temprow)
    filledrow=pd.DataFrame(newdf)   
        
    expandeddf=emptyrow.append(filledrow)        #recombine
    expandeddf.sort_index(inplace=True) #sort
    return expandeddf

 
#%% response and resolve pivottables for excel csv
def generatetagpivtbl(inputdf,columnname, timeinterval):
    #responsepivotdf=generatetagpivtbl(issueschoolexpandeddf,'s_response_bin',[timeframestartdt[0],timeframeenddt[0]])
    #resolvepivotdf=generatetagpivtbl(issueschoolexpandeddf,'s_resolve_bin',[timeframestartdt[0],timeframeenddt[0]])    

    sliceddf=slicebytimeinterval(inputdf,timeinterval)
    numconversations=len(sliceddf.convid.unique())
    
    workindf=sliceddf[['issue',columnname]]
    pivtable=workindf.pivot_table(index='issue', columns=columnname, aggfunc=len, fill_value=0)
    sumoftags=pd.DataFrame(pivtable.transpose().sum())
    pivtable['Total']=sumoftags
    
    return sliceddf, pivtable, numconversations
    
#%% generate pivotables for issues and adminname
def generatetagpivdf(inputdf, columnname, timeinterval):
    #tagpivotdf,responsestats,numconversations=generatetagpivdf(issueschoolexpandeddf,'created_at_Date',[timeframestartdt[0],timeframeenddt[0]])
    #adminpivotdf,responsestats,numconversations=generatetagpivdf(issueschoolexpandeddf,'adminname',[timeframestartdt[0],timeframeenddt[0]])
    sliceddf, pivtable, numconversations=generatetagpivtbl(inputdf,columnname,timeinterval)
    
    #get response stats
    tagRpivotdf=sliceddf[['s_to_first_response',columnname]]    
    tagRpivotdfdes=tagRpivotdf.groupby(columnname).describe()
    tagRpivotdfs=tagRpivotdfdes.unstack().loc[:,(slice(None),['mean','max'])]
    responsestats=tagRpivotdfs['s_to_first_response'].transpose()
    
    return pivtable, responsestats, numconversations

#%% generate pivottables for opentags
def generateopentagpivdf(rawinputdf, timeinterval): #use only sliced, not the augmented one
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    #have to remove those created on the last day of time interval
    
    
    '''
    sliceddf=slicebytimeinterval(rawinputdf,timeinterval)#overallconvdf
    
    #get those currently open earlier than of tfstart
    currentlyopen=rawinputdf[rawinputdf['open']==1]
    openbeforetf=slicebytimeinterval(currentlyopen,[pd.to_datetime(0).date(),tfstart])
    
    #combine for processing
    opentagconvdf=sliceddf.append(openbeforetf)        
    '''
    #set all current open conversations to have last_closed to be time of running script.
    #openconv=rawinputdf[rawinputdf['last_closed'].isnull()]
    rawinputdf.loc[rawinputdf['last_closed'].isnull(), 'last_closed'] = timenow#+pd.timedelta(1,'D')
        
    #get all conversations closed before interval
    closedbefore=slicebytimeinterval(rawinputdf,[pd.to_datetime(0).date(), timeinterval[0]],'last_closed')
    #get all conversations open after interval
    openafter=slicebytimeinterval(rawinputdf,[timeinterval[1],pd.to_datetime(timenow).date()],'created_at')
    outerlapping = closedbefore.merge(openafter, how='outer',on=['convid'])
    opentagconvdf=rawinputdf[~rawinputdf.convid.isin(outerlapping.convid)]
                             
                             
                             
    
    '''     
    #if negative value means issue was closed before end of day. safe
    eodtdelta=opentagconvdf.first_closed.sub(opentagconvdf.created_at_EOD,axis=0)#<---------------usage of first closed misses conversations that have multiple close/openings.
    #forced into 0 bin.
        
    eodbintf=range(1,1+ tfdelta.days )
    eodbin=eodtdelta.apply(lambda s: bintime(s,'D',eodbintf,None))#<--- currently not binning properly
    eodbin[eodbin.isnull()]=(datetime.datetime.now().date()-opentagconvdf.created_at_Date[eodbin.isnull()]).astype('timedelta64[D]')    
    openconvdf=opentagconvdf.assign(eodbin=eodbin.astype(int))
    
    opentagpivotdf=openconvdf[['adminname','created_at_Date','eodbin']].copy()    
    #augment df with duplicates, convert those with None to max based on current date        
    #keep all those that completed within the day 
    closedwithinaday=opentagpivotdf[opentagpivotdf.eodbin==1]    
    #duplicate those that more than 1 day
    closedmorethanaday=opentagpivotdf[opentagpivotdf.eodbin!=1]
    df=[]
    for index, row in closedmorethanaday.iterrows():                           
        for eodval in xrange(row.eodbin):
            temprow=row.copy()#duplicate row
            temprow.created_at_Date=temprow.created_at_Date+pd.Timedelta(eodval+1,'D')
            df.append(temprow)
    tempdf=pd.DataFrame(df)            
    
    opentagpivotdf=closedwithinaday.append(tempdf)
    opentagpivotdf=opentagpivotdf.sort_index()
    
    opentagpivotdfsubset=opentagpivotdf[(opentagpivotdf['created_at_Date']>= tfstart) & (opentagpivotdf['created_at_Date']< tfend)] #hide all those outside timeframe
    opentagpivotdf=opentagpivotdfsubset[['adminname','created_at_Date']].pivot_table(index='adminname', columns='created_at_Date', aggfunc=len, fill_value=0)    
    sumoftags=pd.DataFrame(opentagpivotdf.transpose().sum())            
    
    opentagRpivotdfdes2=opentagpivotdfsubset.groupby('created_at_Date').describe()
    opentagRpivotdfs2=opentagRpivotdfdes2.unstack().loc[:,(slice(None),['count'])]
    opentagRpivotdfs2=opentagRpivotdfs2['eodbin'].transpose()
    opentagpivotdf=opentagpivotdf.append(opentagRpivotdfs2)    
    opentagpivotdf['Total']=sumoftags
    
    
    #get all conversations that last closed is within timeinterval    
    closedin=slicebytimeinterval(rawinputdf,timeinterval,'last_closed')#conversations closed within interval, will miss those open conversations with nat as last closed.
    
    createdin=slicebytimeinterval(rawinputdf,timeinterval,'created_at')#conversations created within interval
    overlappingstart=slicebytimeinterval(rawinputdf,[pd.to_datetime(0).date(), timeinterval[0]],'created_at')#conversations that created and closed overlapping the interval 
    overlappingend=slicebytimeinterval(rawinputdf,[timeinterval[1], pd.to_datetime(timenow)],'last_closed')#conversations that created and closed overlapping the interval 
    overlapping = pd.merge(overlappingstart, overlappingend, how='inner', on=['convid'])
    
    
    sliceddf=sliceddf.merge(currentlyopen,left_index=True, right_index=True)
    
    
    '''
    
    return opentagpivotdf
    
#%% finding the missing tags
def getnonetags(inputdf, timeinterval, tagtype):
    #tfstart=timeinterval[0]
    #tfend=timeinterval[1]
    #tfdelta=tfend-tfstart
    
    sliceddf=slicebytimeinterval(inputdf,timeinterval)
    notag=sliceddf[sliceddf[tagtype]=='None']
    #notag['bodytext']=notag.conversation_message.apply(lambda s: s.body) #<-- soemone conversation_message converted to unicode. lost object properties. probably due to dict conversation or loading from csv
    numconversations=len(notag)
    
    workindf=notag[['adminname','created_at_Date']]
    pivtable=workindf.pivot_table(index='created_at_Date', columns='adminname', aggfunc=len, fill_value=0)
    #sumoftags=pd.DataFrame(pivtable.transpose().sum())
    #pivtable['Total']=sumoftags
        
    return pivtable, notag, numconversations    
    

#%% plot functions 

#%% overallresponse
def overallresponsestatplot(rawinputdf,timeinterval,ofilename):
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    responsestats=slicebytimeinterval(rawinputdf,timeinterval).copy()#overallconvdf
    responsestats=responsestats.sort_values('created_at',ascending=True)
    #convert 'None' str into None #this is a fucking terrible bandaid. Please fix soon
    #responsestats['s_to_first_response']=responsestats.s_to_first_response.apply(lambda s: changenonetotimedeltazero(s))
    responsestats['s_to_last_closed']=responsestats.s_to_last_closed.apply(lambda s: changenonetotimedeltazero(s))
    #responsestats['s_to_last_closed']=responsestats.s_to_last_closed.apply(lambda s: changenattotimedeltazero(s))
    responsestats['s_to_first_closed']=responsestats.s_to_first_closed.apply(lambda s: changenattotimedeltazero(s))
    responsestats['s_to_last_update']=responsestats.s_to_last_update.apply(lambda s: changenonetotimedeltazero(s))
    fr=responsestats['s_to_first_response'].astype('timedelta64[s]')
    fc=responsestats['s_to_first_closed'].astype('timedelta64[s]')
    ls=responsestats['s_to_last_closed'].astype('timedelta64[s]')
    lu=responsestats['s_to_last_update'].astype('timedelta64[s]')
    
    textlst=[]
    for idx,row in responsestats.iterrows():
        adminnamestr='Adminname: ' +str(row.adminname)
        nummessagestr='Number of messages: '+str(row.nummessage)
        numnotestr='Number of notes: '+str(row.numnote)
        numassignstr='Number of assignments: '+str(row.numassign)
        numclosedstr='Number of closed: '+str(row.numclosed)
        schoolstr='School: ' + str(row.school)
        issuestr='Issues: ' + str(row.issue)
        try: 
             usernamestr='Username: ' + str(row.username.encode('utf-8'))
        except AttributeError:
             usernamestr='Username: ' + str(row.username)
        emailstr='Email: ' + str(row.email)        
        textstr='<br>'.join([nummessagestr,issuestr,schoolstr,usernamestr,adminnamestr,emailstr,numnotestr,numassignstr,numclosedstr])#add in conversation id in case need to track back
        textlst.append(textstr)
            
    data1 = Scatter(    x=responsestats['created_at'], y=fr/3600.0,
                        name='First response', mode = 'lines+markers',
                        text=textlst, textposition='top'
                        )
    data2 = Scatter(    x=responsestats['created_at'], y=fc/3600.0,
                        name='First closed', mode = 'lines+markers'
                        )
        
    data3 = Scatter(    x=responsestats['created_at'], y=ls/3600.0,
                        name='Last closed', mode = 'lines+markers'
                        )
    
    data4 = Scatter(    x=responsestats['created_at'], y=lu/3600.0,
                        name='Last update', mode = 'lines+markers'
                        )

    layout = Layout(    title='Overall response for last ' + plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                        yaxis=dict(title='Hours',dtick=5.0),
                        #xaxis=dict(title='Day')
                        xaxis=dict(     rangeselector=dict(
                                        buttons=list([ dict(count=7, label='1w', step='day',stepmode='backward'),
                                                       dict(count=14, label='2w', step='day',stepmode='backward'),
                                                       dict(count=1, label='1m', step='month', stepmode='backward'),
                                                       dict(count=6, label='6m', step='month', stepmode='backward'),
                                                       dict(step='all')
                                                       ])
                                                            ),
                                        rangeslider=dict(),
                                        type='date'
                                      )
                        )
    fig = dict(data=[data1,data2,data3,data4], layout=layout )            
    plot(fig,filename=ofilename)

#%% Open conversations by day
def openconvobytfplot(rawinputdf,timeinterval,ofilename):
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    pivtable=generateopentagpivdf(rawinputdf, timeinterval)
        
    day_piv=pivtable.ix[:-1,:-1]
    convocount=pivtable.ix[-1,:-1]
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
           
    layout = Layout(title='Conversations still open at the end of day for past '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Conversations'),                    
                    barmode='relative',                    
                    xaxis=dict(     rangeselector=dict(
                                        buttons=list([ dict(count=7, label='1w', step='day',stepmode='backward'),
                                                       dict(count=14, label='2w', step='day',stepmode='backward'),
                                                       dict(count=1, label='1m', step='month', stepmode='backward'),
                                                       dict(count=6, label='6m', step='month', stepmode='backward'),
                                                       dict(step='all')
                                                       ])
                                                            ),
                                        rangeslider=dict(),
                                        type='date'
                                      ),
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(day_piv.columns, convocount.values)]
                    )
    fig = dict(data=data_piv, layout=layout )
    plot(fig,filename=ofilename)

#%% Tags by timeframe
def tagsbytfplot(inputdf,timeinterval,ofilename):    #y-axis:time, x-axis tags
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 

    pivtable,responsestats,numconversations=generatetagpivdf(inputdf,'created_at_Date',timeinterval)

    day_piv=pivtable.ix[:,:-1]
    aggstats_piv=responsestats
    aggstats_piv=aggstats_piv.transpose()
    mean_piv=aggstats_piv['mean'].astype('timedelta64[s]')    
    max_piv=aggstats_piv['max'].astype('timedelta64[s]')
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
    
    avgresponse = Scatter(x=day_piv.columns, y=mean_piv/3600.0,
                             name='Average Response time',yaxis='y2')    
    data_piv.append(avgresponse)
    
    longestresponse = Scatter(x=day_piv.columns, y=max_piv/3600.0,
                                 name='Longest Response time', yaxis='y2')    
    data_piv.append(longestresponse)    
        
    layout = Layout(title='Conversations (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Conversations'),
                    xaxis=dict(title='Date'),
                    barmode='relative',
                    yaxis2=dict(title='Time (hours)',titlefont=dict(color='rgb(148, 103, 189)'),
                                      tickfont=dict(color='rgb(148, 103, 189)'),
                                      overlaying='y', side='right'
                                  )
                    )
    fig = dict(data=data_piv, layout=layout )
    plot(fig,filename=ofilename)
    
#%% Plot number of tags for the time interval
def overalltagplot(inputdf,timeinterval,ofilename):#x-axis tags, y-axis number of tags
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 

    pivtable,responsestats,numconversations=generatetagpivdf(inputdf,'created_at_Date',timeinterval)

    #pivtable=inputpivtable[4].copy()#tagpivotdf

    overall_piv=pivtable['Total']
    x=overall_piv.index.tolist()
    y=overall_piv.tolist() 
    datao_piv=[Bar(x=x, y=y)]
    layout = Layout(title='Total conversations (n = '+ str(numconversations) +') split by tags for the last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Number'),
                    xaxis=dict(title='Tags'),
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(x, y)]
                    )
    fig = dict(data=datao_piv, layout=layout )
    plot(fig,filename=ofilename)
    
#%% PLot two overalltagplot for two timeintervals for comparison
def overalltagplot2(inputdf,timeintervallist,ofilename):#dual timeframe comparison. x-axis tags, y-axis number of tags
    datalist=[]
    nlist=[]

    for idx,timeinterval in enumerate(timeintervallist):
        tfstart=timeinterval[0]
        tfend=timeinterval[1]
        tfdelta=tfend-tfstart
        plottf=recogtf(tfdelta,range(tfdelta.days+1)) 

        pivtable,responsestats,numconversations=generatetagpivdf(inputdf,'created_at_Date',timeinterval)
                
        overall_piv=pivtable['Total']
        x=overall_piv.index.tolist()
        y=overall_piv.tolist()
        datao_piv=Bar(x=x, y=y,name=plottf+' ( '+str(tfstart)+' - '+str(tfend)+' )')
        
        datalist.append(datao_piv)
        nlist.append(str(numconversations))
        
    opstr='(' + ','.join(nlist) + ')'
        
    layout = Layout(title='Total conversations split by tags for past two '+ plottf + ', n = ' + opstr,
                    yaxis=dict(title='Number'),
                    xaxis=dict(title='Tags'),
                    barmode='group'
                    )
    fig = dict(data=datalist, layout=layout )
    plot(fig,filename=ofilename)    

#%% Issue handed by admin for the time interval
def allconvobyadminplot(inputdf,timeinterval,ofilename): #need to check numbers. looks wrong*********************
    #allconvobyadminplot(topconvdfcopy,timeinterval,'test.html')
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    pivtable, responsestats, numconversations=generatetagpivdf(inputdf,'adminname',timeinterval)
    
    #pivtable=inputpivtable[5].copy()#adminpivotdf
               
    day_piv=pivtable.ix[:,:-1]
    aggstats_piv=responsestats
    aggstats_piv=aggstats_piv.transpose()
    mean_piv=aggstats_piv['mean'].astype('timedelta64[s]')    
    max_piv=aggstats_piv['max'].astype('timedelta64[s]')
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
    
    avgresponse = Scatter(x=day_piv.columns, y=mean_piv/3600.0,
                             name='Average Response time',yaxis='y2')    
    data_piv.append(avgresponse)
    
    longestresponse = Scatter(x=day_piv.columns, y=max_piv/3600.0,
                                 name='Longest Response time', yaxis='y2')    
    data_piv.append(longestresponse)
        
    layout = Layout(title='Conversations (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Conversations'),
                    xaxis=dict(title='Admin name'),
                    barmode='relative',
                    yaxis2=dict(title='Time(hours)',titlefont=dict(color='rgb(148, 103, 189)'),
                                      tickfont=dict(color='rgb(148, 103, 189)'),
                                      overlaying='y', side='right'
                                  )
                    )
    fig = dict(data=data_piv, layout=layout )
    plot(fig,filename=ofilename)
    
#%% Tags by timeframe
    #tagsbyschoolplot(expandtag(expandtag(topconvdfcopy,'issue'),'school'),timeinterval,'test.html')
def tagsbyschoolplot(inputdf,timeinterval,ofilename):
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    
    sliceddf=slicebytimeinterval(inputdf,timeinterval)
    #remove untagged schools
    sliceddf=sliceddf[sliceddf['school']!='None']
    
    workindf=sliceddf[['issue','school']]#used in tagsbyschoolplot
    pivtable=workindf.pivot_table(index='issue', columns='school', aggfunc=len, fill_value=0)
    #pivtable=inputpivtable[3].copy()       #groupbyschool     
    
    plottf=recogtf(tfdelta,range(tfdelta.days+1))    
    n=len(sliceddf.convid.unique())   
    day_piv=pivtable    
        
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)        
        
    layout = Layout(title='Conversation Tags by School (n = '+ str(n) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Tags'),
                    xaxis=dict(title='School'),
                    barmode='relative'                    
                    )
    fig = dict(data=data_piv, layout=layout )
    plot(fig,filename=ofilename)
#%% nontag plot
def nonetagplot(inputdf, timeinterval,columnname,ofilename):
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    pivtable, notag, numconversations = getnonetags(inputdf, timeinterval, columnname)
        
    notag=notag.sort_values('created_at',ascending=True)        
    data_piv=[]  
    
    groupedbyadminname=notag.groupby('adminname')
    admincounter=0
    for groupname, item in groupedbyadminname:
        textlst=[]        
        for idx,row in item.iterrows():
             adminnamestr='Adminname: ' +str(groupname)
             #bodystr='Text: ' + str(row.conversation_message.body.encode('utf-8'))
             try: 
                  usernamestr='Username: ' + str(row.username.encode('utf-8'))
             except AttributeError:
                  usernamestr='Username: ' + str(row.username)
             emailstr='Email: ' + str(row.email)
             convid='Convid: ' + str(row.convid)
             
             textstr='<br>'.join([adminnamestr,usernamestr,emailstr,convid])#add in conversation id in case need to trace back
             textlst.append(textstr)
        
        yval=np.zeros(len(item))   
        yval.fill(len(groupedbyadminname)-admincounter)
             
        tempdata_piv=Scatter(   x=item['created_at'], y=yval, mode = 'markers',
                   name=str(groupname), text=textlst, textposition='top'
                   )
        data_piv.append(tempdata_piv)
        admincounter+=1
        
    layout = Layout(title='Conversations not tagged in '+columnname+' (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Conversations status'),
                    xaxis=dict(title='Date')                
                    )
    fig = dict(data=data_piv, layout=layout)
    plot(fig,filename=ofilename)
    
    data_piv2=[]    
    for idx,row in pivtable.iterrows():
        tempdata_piv = Bar(x=pivtable.columns, y=row.values, name=idx)
        data_piv2.append(tempdata_piv)
        
    layout2 = Layout(title='Conversations not tagged in '+columnname+' (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend)+' )',
                    yaxis=dict(title='Conversation date'),
                    xaxis=dict(title='Adminname'),barmode='relative'               
                    )
    fig = dict(data=data_piv2, layout=layout2)
    plot(fig,filename=ofilename+'byadmin')
        
#%% Plotting
#%%group by tf
print('Generating plots')
#timeframe=[7,30,180,365]
timeframeend=[0,8,0,31,0,0]#[w1,w2,m1,m2,0.5y,1y]
timeframestart=[7,15,30,61,180,365]
#timeframe=[7]
#timeframedt=[timenow.date()-datetime.timedelta(dt) for dt in timeframe]
timeframestartdt=[timenow.date()-datetime.timedelta(dt) for dt in timeframestart]
timeframeenddt=[timenow.date()-datetime.timedelta(dt) for dt in timeframeend]
             
#change none to string so that can group
topconvdfcopy['issue']=topconvdfcopy.issue.apply(lambda s: changenonetostr(s))
#change none to string so that can group
topconvdfcopy['school']=topconvdfcopy.school.apply(lambda s: changenonetostr(s))

issueschoolexpandeddf=expandtag(expandtag(topconvdfcopy,'issue'),'school')

#save folders
foldername=timenow.strftime("%Y%m%d-%H%M%S")
pathbackup=os.path.abspath(os.path.join(outputfolder,foldername))     

try: 
    os.makedirs(pathbackup)
except OSError:
    if not os.path.isdir(pathbackup):
        raise


outputstats=True
if outputstats:
    sliceddf_resp, responsepivotdf,numconversations=generatetagpivtbl(issueschoolexpandeddf,'s_response_bin',[timeframestartdt[0],timeframeenddt[0]])
    sliceddf_resolv, resolvepivotdf,numconversations=generatetagpivtbl(issueschoolexpandeddf,'s_resolve_bin',[timeframestartdt[0],timeframeenddt[0]])  
    tagpivotdf,responsestats,numconversations=generatetagpivdf(issueschoolexpandeddf,'created_at_Date',[timeframestartdt[0],timeframeenddt[0]])
    
    response_csv_path=os.path.abspath(os.path.join(pathbackup,'response.csv'))
    with open(response_csv_path, 'w') as f:
        responsepivotdf.to_csv(f,sep='\t')
    
    resolve_csv_path=os.path.abspath(os.path.join(pathbackup,'resolve.csv'))        
    with open(resolve_csv_path, 'w') as f:
        resolvepivotdf.to_csv(f,sep='\t')        
        
    dailytagcount_csv_path=os.path.abspath(os.path.join(pathbackup,'dailytagcount.csv'))            
    with open(dailytagcount_csv_path, 'w') as f: 
        tagpivotdf.to_csv(f,sep='\t')      

plotallconvobyadmin=True
if plotallconvobyadmin:        
    allconvobyadminplot(issueschoolexpandeddf,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(pathbackup,'tagsbyAdmin.html')))

plotoveralltags=True
if plotoveralltags:
    overalltagplot(issueschoolexpandeddf,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(pathbackup,'Overalltagsformonth.html')))
    overalltagplot(issueschoolexpandeddf,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(pathbackup,'Overalltagsforweek.html')))

    overalltagplot2(issueschoolexpandeddf,[[timeframestartdt[2],timeframeenddt[2]],[timeframestartdt[3],timeframeenddt[3]]],os.path.abspath(os.path.join(pathbackup,'Overalltagsforpast2month.html')))
    overalltagplot2(issueschoolexpandeddf,[[timeframestartdt[0],timeframeenddt[0]],[timeframestartdt[1],timeframeenddt[1]]],os.path.abspath(os.path.join(pathbackup,'Overalltagsforpast2week.html')))
            
plotopenconvobytf=True    
if plotopenconvobytf:
    openconvobytfplot(topconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(pathbackup,'openbyday_1W.html')))    
    openconvobytfplot(topconvdfcopy,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(pathbackup,'openbyday_1M.html')))
    
plottagsbyday=True
if plottagsbyday:    
    tagsbytfplot(issueschoolexpandeddf,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(pathbackup,'tagsbyday.html')))
            
plotoverallresponsestats=True
if plotoverallresponsestats:
    overallresponsestatplot(topconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(pathbackup,'overallresponse_1W.html')))
    overallresponsestatplot(topconvdfcopy,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(pathbackup,'overallresponse_1M.html')))
        
    #overallresponsestatplot(stats[2][7],'overallresponse_6M.html','6 Months')
    #overallresponsestatplot(stats[3][7],'overallresponse_1Y.html','Year') 

plottagsbyschool=True
if plottagsbyschool:
    tagsbyschoolplot(issueschoolexpandeddf,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(pathbackup,'tagsbyschool_1W.html')))
    tagsbyschoolplot(issueschoolexpandeddf,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(pathbackup,'tagsbyschool_1M.html')))
    tagsbyschoolplot(issueschoolexpandeddf,[timeframestartdt[5],timeframeenddt[5]],os.path.abspath(os.path.join(pathbackup,'tagsbyschool_1Y.html')))
    

plotnonetags=True
if plotnonetags:
    nonetagplot(topconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],'issue',os.path.abspath(os.path.join(pathbackup,'missingissue_1W.html')))
    nonetagplot(topconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],'school',os.path.abspath(os.path.join(pathbackup,'missingschool_1W')))
    
'''
ilifetimestats=dict()
lastweekstats=dict()
lastmonthstats=dict()
lasthalfyearstats=dict()
lastyearstats=dict()

#statslist=[lastweekstats,lastmonthstats,lasthalfyearstats,lastyearstats]

timeframe=[7,30,180,365]
timeframedt=[timenow.date()-datetime.timedelta(dt) for dt in timeframe]
responseattrs=['s_to_first_response','s_to_first_closed','s_to_last_closed','s_to_last_update']

def getstats(df,tf):
     temptimedf=df.loc[item['created_at_Date'] >= tf]
     timestats=temptimedf[responseattrs].describe()
     numconv=len(temptimedf)
     numopen=temptimedf['open'].sum()
     #top # tags
     #top schools
     return {'data':temptimedf,'stats':timestats,'numconv':numconv,'numopen':numopen}
'''
#%% group by admin
#adminconvdfcopy.loc['Sun'].describe()
#adminconvdfcopy[['s_to_first_response','s_to_first_closed','s_to_last_closed']]

#generate a dataframe?
#for person,stat in lastweekstats.iteritems(): 
#     stat['stats']
if toplot:
    #keep copy for printing 
    groupedbyadminname = topconvdfcopy.copy().groupby('adminname')    
    for i, item in groupedbyadminname:
        toprint=groupedbyadminname.get_group(i).sort_values('created_at',ascending=True)
        toprint['s_to_first_response']=toprint.s_to_last_closed.apply(lambda s: changenonetotimedeltazero(s))
        toprint['s_to_last_closed']=toprint.s_to_last_closed.apply(lambda s: changenonetotimedeltazero(s))
        toprint['s_to_first_closed']=toprint.s_to_first_closed.apply(lambda s: changenattotimedeltazero(s))
        toprint['s_to_last_update']=toprint.s_to_last_update.apply(lambda s: changenonetotimedeltazero(s))
        fr=toprint['s_to_first_response'].astype('timedelta64[s]')
        fc=toprint['s_to_first_closed'].astype('timedelta64[s]')
        ls=toprint['s_to_last_closed'].astype('timedelta64[s]')
        lu=toprint['s_to_last_update'].astype('timedelta64[s]')
#        ydata=[fr,fc,ls,lu]
        textlst=[]
        for idx,row in item.iterrows():
            adminnamestr='Adminname: ' +str(row.adminname)
            nummessagestr='Number of messages: '+str(row.nummessage)
            numnotestr='Number of notes: '+str(row.numnote)
            numassignstr='Number of assignments: '+str(row.numassign)
            numclosedstr='Number of closed: '+str(row.numclosed)
            schoolstr='School: ' + str(row.school)
            issuestr='Issues: ' + str(row.issue)
            try: 
                 usernamestr='Username: ' + str(row.username.encode('utf-8'))
            except AttributeError:
                 usernamestr='Username: ' + str(row.username)
            emailstr='Email: ' + str(row.email)        
            textstr='<br>'.join([nummessagestr,issuestr,schoolstr,usernamestr,adminnamestr,emailstr,numnotestr,numassignstr,numclosedstr])
            textlst.append(textstr)
        
        data1 = Bar(    x=toprint['created_at'], # assign x as the dataframe column 'x'
                        y=fr/3600.0,
                        name='First response'                        
                        )
        
        data2 = Bar(    x=toprint['created_at'], # assign x as the dataframe column 'x'
                        y=(fc-fr)/3600.0,
                        name='First closed'
                        )
        
        data3 = Bar(    x=toprint['created_at'], # assign x as the dataframe column 'x'
                        y=(ls-fc)/3600.0,
                        name='Last closed'
                        )
        
        data4 = Bar(    x=toprint['created_at'], # assign x as the dataframe column 'x'
                        y=(lu-ls)/3600.0,
                        name='Last update'
                        )
        data5 = Scatter(x=toprint['created_at'], y=fr/3600.0,
                        name='First response', mode = 'markers',
                        text=textlst, textposition='top',
                        marker= dict(size=0,opacity= 0)
                        )
        
        layout = Layout(title=i+'\'s Stats',
                        yaxis=dict(title='Response Time (Hours)'),
                        xaxis=dict(title='Date/Day/Time',
                                   rangeselector=dict(
                                        buttons=list([ dict(count=7, label='1w', step='day',stepmode='backward'),
                                                       dict(count=14, label='2w', step='day',stepmode='backward'),
                                                       dict(count=1, label='1m', step='month', stepmode='backward'),
                                                       dict(count=6, label='6m', step='month', stepmode='backward'),
                                                       dict(step='all')
                                                       ])
                                                            ),
                                    ),
                        barmode='relative'                        
                        )
        fig = dict( data=[data1,data2,data3,data4,data5], layout=layout )        
        plot(fig,filename=os.path.abspath(os.path.join(outputfolder,i+'.html')))
    
    
    
    
#fig = go.Figure(data=data, layout=layout)
#url = py.plot(fig, filename='pandas/line-plot-title')
     
'''
datetime.datetime.now().date()

#use get_group(key) to pull individuals out
groupedbyadminstats=grouped.describe()
groupedbyadmincol=['s_to_first_response','s_to_first_closed','s_to_last_closed','s_to_last_update']
groupedbyadminstats=groupedbyadminstats[groupedbyadmincol]
grouped2=adminconvdfcopy.groupby(['adminname', 'convid'])

groupedbyadmindate=adminconvdfcopy[['adminname','created_at','first_response','s_to_first_response']].groupby(['adminname', 'first_response','s_to_first_response'])
groupedbyadmindate=adminconvdfcopy[['adminname','created_at','first_response','s_to_first_response']].groupby(adminconvdfcopy['created_at'].map(lambda x: x.day))
groupedbyadmindatesummary=groupedbyadmindate.describe()
'''
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