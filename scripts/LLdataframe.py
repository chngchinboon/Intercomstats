# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 16:19:36 2016
########################################################################################################
#Current bugs
########################################################################################################
#Possible errors in overalresponse
#Possible errors in merging/updating local database



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
from plotly.offline import download_plotlyjs, plot
#from plotly.graph_objs import *
from plotly.graph_objs import Bar, Layout, Scatter 

import numpy as np
import pandas as pd
from ast import literal_eval
import tictocgen as tt
import xlsxwriter

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

#Intercom.app_id = 
#Intercom.app_api_key = ''

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
from intercom import Admin
admindf=pd.DataFrame([x.__dict__ for x in Admin.all()]) 
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
from intercom import Count
AdminCount=pd.DataFrame(Count.conversation_counts_for_each_admin)
print('Retrieved AdminCount Df from Intercom')
#%% Get tags
from intercom import Tag
tagdf=pd.DataFrame([x.__dict__ for x in Tag.all()])

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
from intercom import User
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
         toaugment['s_response_bin']=toaugment.s_to_first_response.apply(lambda s: bintime(s,'h',[1,2,3,4,5],0))
         #can't print if type is replaced with str None
         #Have to fill nattype with none first #this screws with plotly.
         #toaugment['s_to_last_closed'] = toaugment.s_to_last_closed.apply(lambda x: x if isinstance(x, pd.tslib.Timedelta) 
         #                                      and not isinstance(x, pd.tslib.NaTType) else 'None')    
         toaugment['s_resolve_bin']=toaugment.s_to_last_closed.apply(lambda s: bintime(s,'h',[1,2,3,4,5,10,24],0))
    
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
    sliceddf=df[(df[column] >= pd.to_datetime(timeinterval[0])) & (df[column] < pd.to_datetime(timeinterval[1]))]
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
    sumoftagsbycolumn=pd.DataFrame(pivtable.sum(),columns=['Total'])
    pivtable=pivtable.append(sumoftagsbycolumn.transpose())
    
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
    df=rawinputdf.copy()
    
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
    df.loc[df['last_closed'].isnull(), 'last_closed'] = timenow#+pd.timedelta(1,'D')
        
    #get all conversations closed before interval
    closedbefore=slicebytimeinterval(df,[pd.to_datetime(0).date(), timeinterval[0]],'last_closed')
    #get all conversations open after interval
    openafter=slicebytimeinterval(df,[timeinterval[1],pd.to_datetime(timenow).date()],'created_at')
    outerlapping = closedbefore.merge(openafter, how='outer',on=['convid'])
    opentagconvdf=df[~df.convid.isin(outerlapping.convid)]
                             
    
    #generate EOD for each day within interval for checking.                            
    EODlist=pd.date_range(start=pd.to_datetime(tfstart)+pd.Timedelta('1 days')+pd.Timedelta('-1us'), periods=tfdelta.days).tolist()
    
    #iterate through each conversation for each EOD to check if open
    openEOD=[]
    totalconv=[]
    for EOD in EODlist:
            convopenatEOD=opentagconvdf[(EOD>opentagconvdf.created_at) & (EOD<opentagconvdf.last_closed)]                                        
            #get counts
            groupedbyadminname=convopenatEOD[['adminname','convid']].groupby('adminname').aggregate(len)
            groupedbyadminname.rename(columns={"convid": 'count'},inplace=True)
            
            numconv=groupedbyadminname.values.sum()
            #groupedbyadminname
            dicttoappend=groupedbyadminname['count'].to_dict()
            #dicttoappend['Total']=numconv
            openEOD.append(dicttoappend)
            totalconv.append(numconv)
            
    openEODdf=pd.DataFrame(openEOD,index=pd.to_datetime(EODlist).date).transpose()
    
    openEODdf=openEODdf.fillna(value=0)
    
    totalconvdf=pd.DataFrame(totalconv,index=pd.to_datetime(EODlist).date,columns=['Total']).transpose()
    opentagpivotdf=openEODdf.append(totalconvdf)
    
    
    
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

#%% overallresponse #buggy need check!!!
def overallresponsestatplot(rawinputdf,timeinterval,ofilename,silent=False):
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
        numopenstr='Number of opened: '+str(row.numopened)
        schoolstr='School: ' + str(row.school)
        issuestr='Issues: ' + str(row.issue)
        if bool(row.open):            
            currstatus='Current status: Open'
        else:
            currstatus='Current status: Closed'
        try: 
             usernamestr='Username: ' + str(row.username.encode('utf-8'))
        except AttributeError:
             usernamestr='Username: ' + str(row.username)
        emailstr='Email: ' + str(row.email)        
        textstr='<br>'.join([nummessagestr,issuestr,schoolstr,usernamestr,adminnamestr,emailstr,numnotestr,numassignstr,numclosedstr,numopenstr,currstatus])#add in conversation id in case need to track back
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

    layout = Layout(    title='Overall response for last ' + plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
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
                                        #rangeslider=dict(),
                                        type='date'
                                      )
                        )
    fig = dict(data=[data1,data2,data3,data4], layout=layout )            
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)

#%% Open conversations by day
def openconvobytfplot(rawinputdf,timeinterval,ofilename,silent=False):
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    pivtable=generateopentagpivdf(rawinputdf, timeinterval)
        
    day_piv=pivtable.ix[:-1,:]
    convocount=pivtable.ix[-1,:]
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
           
    layout = Layout(title='Conversations still open at the end of day for past '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
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
                                        #rangeslider=dict(),
                                        type='date'
                                      ),
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(day_piv.columns, convocount.values)]
                    )
    fig = dict(data=data_piv, layout=layout )
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)

#%% Tags by timeframe
def tagsbytfplot(inputdf,timeinterval,ofilename,silent=False):    #y-axis:time, x-axis tags
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 

    pivtable,responsestats,numconversations=generatetagpivdf(inputdf,'created_at_Date',timeinterval)

    day_piv=pivtable.ix[:-1,:-1]
    aggstats_piv=responsestats
    aggstats_piv=aggstats_piv.transpose()
    #mean_piv=aggstats_piv['mean'].astype('timedelta64[s]')    
    #max_piv=aggstats_piv['max'].astype('timedelta64[s]')
    convocount=pivtable.ix[-1,:]#last row
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
    '''
    avgresponse = Scatter(x=day_piv.columns, y=mean_piv/3600.0,
                             name='Average Response time',yaxis='y2')    
    data_piv.append(avgresponse)
    
    longestresponse = Scatter(x=day_piv.columns, y=max_piv/3600.0,
                                 name='Longest Response time', yaxis='y2')    
    data_piv.append(longestresponse)    
    '''    
    layout = Layout(title='Conversations (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
                    yaxis=dict(title='Conversations'),
                    xaxis=dict(title='Date'),
                    barmode='relative',
                    #yaxis2=dict(title='Time (hours)',titlefont=dict(color='rgb(148, 103, 189)'),
                    #                  tickfont=dict(color='rgb(148, 103, 189)'),
                    #                  overlaying='y', side='right'
                    #              )
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(day_piv.columns, convocount.values)]
                    )
    fig = dict(data=data_piv, layout=layout )
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)
    
#%% Plot number of tags for the time interval
def overalltagplot(inputdf,timeinterval,ofilename,silent=False):#x-axis tags, y-axis number of tags
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 

    pivtable,responsestats,numconversations=generatetagpivdf(inputdf,'created_at_Date',timeinterval)

    #pivtable=inputpivtable[4].copy()#tagpivotdf

    overall_piv=pivtable['Total'][:-1]
    x=overall_piv.index.tolist()
    y=overall_piv.tolist() 
    datao_piv=[Bar(x=x, y=y)]
    layout = Layout(title='Total conversations (n = '+ str(numconversations) +') split by tags for the last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
                    yaxis=dict(title='Number'),
                    xaxis=dict(title='Tags'),
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(x, y)]
                    )
    fig = dict(data=datao_piv, layout=layout )
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)
    
#%% PLot two overalltagplot for two timeintervals for comparison
def overalltagplot2(inputdf,timeintervallist,ofilename,silent=False):#dual timeframe comparison. x-axis tags, y-axis number of tags
    datalist=[]
    nlist=[]

    for idx,timeinterval in enumerate(timeintervallist):
        tfstart=timeinterval[0]
        tfend=timeinterval[1]
        tfdelta=tfend-tfstart
        plottf=recogtf(tfdelta,range(tfdelta.days+1)) 

        pivtable,responsestats,numconversations=generatetagpivdf(inputdf,'created_at_Date',timeinterval)
                
        overall_piv=pivtable['Total'][:-1]
        x=overall_piv.index.tolist()
        y=overall_piv.tolist()
        datao_piv=Bar(x=x, y=y,name=plottf+' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )')
        
        datalist.append(datao_piv)
        nlist.append(str(numconversations))
        
    opstr='(' + ','.join(nlist) + ')'
        
    layout = Layout(title='Total conversations split by tags for past two '+ plottf + ', n = ' + opstr,
                    yaxis=dict(title='Number'),
                    xaxis=dict(title='Tags'),
                    barmode='group'
                    )
    fig = dict(data=datalist, layout=layout )
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)
    

#%% Issue handed by admin for the time interval
def allconvobyadminplot(inputdf,timeinterval,ofilename,silent=False): #need to check numbers. looks wrong*********************
    #allconvobyadminplot(topconvdfcopy,timeinterval,'test.html')
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    pivtable, responsestats, numconversations=generatetagpivdf(inputdf,'adminname',timeinterval)
    
    #pivtable=inputpivtable[5].copy()#adminpivotdf
               
    day_piv=pivtable.ix[:-1,:-1]
    aggstats_piv=responsestats
    aggstats_piv=aggstats_piv.transpose()
    #mean_piv=aggstats_piv['mean'].astype('timedelta64[s]')    
    #max_piv=aggstats_piv['max'].astype('timedelta64[s]')
    convocount=pivtable.ix[-1,:]
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
    '''
    avgresponse = Scatter(x=day_piv.columns, y=mean_piv/3600.0,
                             name='Average Response time',yaxis='y2')    
    data_piv.append(avgresponse)
    
    longestresponse = Scatter(x=day_piv.columns, y=max_piv/3600.0,
                                 name='Longest Response time', yaxis='y2')    
    data_piv.append(longestresponse)
    '''    
    layout = Layout(title='Conversations (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
                    yaxis=dict(title='Conversations'),
                    xaxis=dict(title='Admin name'),
                    barmode='relative',
                    #yaxis2=dict(title='Time(hours)',titlefont=dict(color='rgb(148, 103, 189)'),
                    #                  tickfont=dict(color='rgb(148, 103, 189)'),
                    #                  overlaying='y', side='right'
                    #              )
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(day_piv.columns, convocount.values)]
                    )
    fig = dict(data=data_piv, layout=layout )
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)
    
#%% Tags by timeframe
    #tagsbyschoolplot(expandtag(expandtag(topconvdfcopy,'issue'),'school'),timeinterval,'test.html')
def tagsbyschoolplot(inputdf,timeinterval,ofilename,silent=False):
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
        
    layout = Layout(title='Conversation Tags by School (n = '+ str(n) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
                    yaxis=dict(title='Tags'),
                    xaxis=dict(title='School'),
                    barmode='relative'                    
                    )
    fig = dict(data=data_piv, layout=layout )
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)
#%% nontag plot
def nonetagplot(inputdf, timeinterval,columnname,ofilename,silent=False):
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
        
    layout = Layout(title='Conversations not tagged in '+columnname+' (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
                    yaxis=dict(title='Conversations status'),
                    xaxis=dict(title='Date')                
                    )
    fig = dict(data=data_piv, layout=layout)
    if not silent:
        plot(fig,filename=ofilename+'.html')
    else:
        plot(fig,filename=ofilename+'.html',auto_open=False)
    
    data_piv2=[]    
    for idx,row in pivtable.iterrows():
        tempdata_piv = Bar(x=pivtable.columns, y=row.values, name=idx)
        data_piv2.append(tempdata_piv)
        
    layout2 = Layout(title='Conversations not tagged in '+columnname+' (n = '+ str(numconversations) +') for last '+ plottf + ' ( '+str(tfstart)+' - '+str(tfend-pd.Timedelta('1 day'))+' )',
                    yaxis=dict(title='Conversation date'),
                    xaxis=dict(title='Adminname'),barmode='relative'               
                    )
    fig = dict(data=data_piv2, layout=layout2)
    if not silent:
        plot(fig,filename=ofilename+'byadmin.html')
    else:
        plot(fig,filename=ofilename+'byadmin.html',auto_open=False)
        
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

#for debugging
#timeinterval=[timeframestartdt[0],timeframeenddt[0]]
#ofilename='test'                
             
#change none to string so that can group
topconvdfcopy['issue']=topconvdfcopy.issue.apply(lambda s: changenonetostr(s))
#change none to string so that can group
topconvdfcopy['school']=topconvdfcopy.school.apply(lambda s: changenonetostr(s))

issueschoolexpandeddf=expandtag(expandtag(topconvdfcopy,'issue'),'school')

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
        temptopconvdfcopy=topconvdfcopy[(topconvdfcopy.adminname.isin(admindfbycountry[idx].name))|(topconvdfcopy.adminname.isnull())] 
    else:
        tempexpanded=issueschoolexpandeddf[(issueschoolexpandeddf.adminname.isin(admindfbycountry[idx].name))]                                            
        temptopconvdfcopy=topconvdfcopy[(topconvdfcopy.adminname.isin(admindfbycountry[idx].name))]                                            
    
    subfolderpath=os.path.abspath(os.path.join(outputfolder,foldername,country))     
    
    try: 
        os.makedirs(subfolderpath)
    except OSError:
        if not os.path.isdir(subfolderpath):
            raise
    
    outputstats=True
    if outputstats & ~Alloutdisable:
        sliceddf_resp, responsepivotdf,numconversations=generatetagpivtbl(tempexpanded,'s_response_bin',[timeframestartdt[0],timeframeenddt[0]])
        sliceddf_resp2, responsepivotdf2,numconversations2=generatetagpivtbl(tempexpanded,'s_response_bin',[timeframestartdt[1],timeframeenddt[1]])
        sliceddf_resolv, resolvepivotdf,numconversations=generatetagpivtbl(tempexpanded,'s_resolve_bin',[timeframestartdt[0],timeframeenddt[0]])  
        tagpivotdf,responsestats,numconversations=generatetagpivdf(tempexpanded,'created_at_Date',[timeframestartdt[0],timeframeenddt[0]])
        
        #modify the results to look like AGP. possibly want to shift it to within function?
        totalconvthisweek=responsepivotdf['Total'][-1]
        uniquedresp=len(sliceddf_resp.convid.unique())    
        totalconvpreviousweek=responsepivotdf2['Total'][-1]
        uniquedresp2=len(sliceddf_resp2.convid.unique())
        responsepivotdf['%']=responsepivotdf['Total'].apply(lambda s: float(s)/totalconvthisweek*100)#get percentage of total
        
        #try:
        #    within4hours=float(responsepivotdf['Total'][-1]-responsepivotdf[5][-1])/totalconvthisweek*100
            
        within4hours=[]
        for i in xrange(4): 
            try:
                within4hours.append(responsepivotdf[i+1].ix['Total'])
            except KeyError:
                print('Missing response timebin: ' + str(i+1))
                pass
        within4hours=float(sum(within4hours))/totalconvthisweek*100
        #except KeyError:
        #    within4hours=None
        unresolvedthisweek=resolvepivotdf[0]['Total']
        uniquedunresolved=len(sliceddf_resolv[sliceddf_resolv['s_resolve_bin']==0].convid.unique())
        
        #rename so that column labels make sense
        responsepivotdf.rename(columns={1: '0-1', 2: '1-2', 3:'2-3',4:'3-4',5:'>4','Total':'Grand Total',0:'UN'}, inplace=True)
        cols=resolvepivotdf.columns.tolist()
        if cols[0]==0: #handle when there are unresolved conversations
            cols=cols[1:-1]+[cols[0]]+[cols[-1]]
            resolvepivotdf=resolvepivotdf[cols]
                
        resolvepivotdf.rename(columns={1: '0-1', 2: '1-2', 3:'2-3',4:'3-4',10:'4-10',24:'>24',0:'UN','Total':'Grand Total'}, inplace=True)
        
        #generate files
        outputexcelpath=os.path.abspath(os.path.join(subfolderpath,'Weeklyemail.xlsx'))
           
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(outputexcelpath, engine='xlsxwriter')
                        
        # Convert the dataframe to an XlsxWriter Excel object.
        responserow=5
        responsepivotdf.to_excel(writer, sheet_name='Sheet1',startrow=responserow)
        
        workbook  = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.write_string(0, 0,'Weekly Email Support Summary')
        worksheet.write_string(3, 0,'Email Response')        
        merge_format = workbook.add_format({
                                                #'bold':     True,
                                                #'border':   6,
                                                'align':    'center',
                                                'valign':   'vcenter'#,
                                                #'fg_color': '#D7E4BC',
                                            })    
        #merge_range(first_row, first_col, last_row, last_col, data[, cell_format])
        worksheet.merge_range(responserow-1,1,responserow-1,7, 'No. of hours taken to Respond',merge_format)
        worksheet.write_string(responserow-1, 0,'Category')
        if within4hours:
            worksheet.write_string(responserow+len(responsepivotdf)+2, 4, "{:.2f}".format(within4hours)+'% responded within 4hrs')
        else:
            worksheet.write_string(responserow+len(responsepivotdf)+2, 4, "{:.2f}".format(within4hours)+'% responded within 4hrs')

        
        summaryrow=responserow+len(responsepivotdf)+3
        worksheet.write_string(summaryrow, 0,'Summary:')
        worksheet.write_string(summaryrow+1, 0,'1) Total of ' + str(totalconvthisweek) + ' ('+ str(uniquedresp) +' conversations) email support cases. (Prev week: ' + str(totalconvpreviousweek) + ' ('+ str(uniquedresp2) +' conversations))')
        worksheet.write_string(summaryrow+2, 0,'2) Unresolved emails: ' + str(unresolvedthisweek)+' ('+str(uniquedunresolved)+' conversations)') 
                
        worksheet.write_string(summaryrow+6, 0,'Email Resolve')
        
        resolverow=summaryrow+7
        worksheet.write_string(resolverow-1, 0,'Category')
        worksheet.merge_range(resolverow-1,1,resolverow-1,8, 'No. of Hours taken to Resolve',merge_format)
        
        resolvepivotdf.to_excel(writer, sheet_name='Sheet1',startrow=resolverow)
        tagpivotdf.to_excel(writer, sheet_name='Sheet1',startrow=resolverow+len(resolvepivotdf)+20)
        format1=workbook.add_format({'font_color': 'white'})
        worksheet.conditional_format(responserow+1,1,responserow+len(responsepivotdf),len(responsepivotdf.columns), {'type':     'cell',
                                        'criteria': '=',
                                        'value':    0,
                                        'format':   format1})
        worksheet.conditional_format(resolverow+1,1,resolverow+len(resolvepivotdf),len(resolvepivotdf.columns), {'type':     'cell',
                                        'criteria': '=',
                                        'value':    0,
                                        'format':   format1})
        
        # Close the Pandas Excel writer and output the Excel file.
        writer.save()    
        
        #response_csv_path=os.path.abspath(os.path.join(subfolderpath,'response.csv'))        
        #with open(response_csv_path, 'w') as f:
        #    responsepivotdf.to_csv(f,sep='\t')
         
        
        #resolve_csv_path=os.path.abspath(os.path.join(subfolderpath,'resolve.csv'))        
        #with open(resolve_csv_path, 'w') as f:
        #    resolvepivotdf.to_csv(f,sep='\t')        
            
        #dailytagcount_csv_path=os.path.abspath(os.path.join(subfolderpath,'dailytagcount.csv'))            
        #with open(dailytagcount_csv_path, 'w') as f: 
        #    tagpivotdf.to_csv(f,sep='\t')      
    
    plotallconvobyadmin=True
    if plotallconvobyadmin & ~Alloutdisable:        
        allconvobyadminplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'tagsbyAdmin_1W_'+country)),silent=pltsilent[idx])
    
    plotoveralltags=True
    if plotoveralltags & ~Alloutdisable:
        overalltagplot(tempexpanded,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsformonth_'+country)),silent=pltsilent[idx])
        overalltagplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsforweek_'+country)),silent=pltsilent[idx])
    
        overalltagplot2(tempexpanded,[[timeframestartdt[3],timeframeenddt[3]],[timeframestartdt[2],timeframeenddt[2]]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsforpast2month_'+country)),silent=pltsilent[idx])
        overalltagplot2(tempexpanded,[[timeframestartdt[1],timeframeenddt[1]],[timeframestartdt[0],timeframeenddt[0]]],os.path.abspath(os.path.join(subfolderpath,'Overalltagsforpast2week_'+country)),silent=pltsilent[idx])
                
    plotopenconvobytf=True    
    if plotopenconvobytf & ~Alloutdisable:
        openconvobytfplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'openbyday_1W_'+country)),silent=pltsilent[idx])    
        openconvobytfplot(temptopconvdfcopy,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'openbyday_1M_'+country)),silent=pltsilent[idx])
        
    plottagsbyday=True
    if plottagsbyday & ~Alloutdisable:    
        tagsbytfplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'tagsbyday_1W_'+country)),silent=pltsilent[idx])
                
    plotoverallresponsestats=True
    if plotoverallresponsestats & ~Alloutdisable:
        overallresponsestatplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'overallresponse_1W_'+country)),silent=pltsilent[idx])
        overallresponsestatplot(temptopconvdfcopy,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'overallresponse_1M_'+country)),silent=pltsilent[idx])
    
    plottagsbyschool=True
    if plottagsbyschool & ~Alloutdisable:
        try:
            tagsbyschoolplot(tempexpanded,[timeframestartdt[0],timeframeenddt[0]],os.path.abspath(os.path.join(subfolderpath,'tagsbyschool_1W_'+country)),silent=pltsilent[idx])
            tagsbyschoolplot(tempexpanded,[timeframestartdt[2],timeframeenddt[2]],os.path.abspath(os.path.join(subfolderpath,'tagsbyschool_1M_'+country)),silent=pltsilent[idx])
            tagsbyschoolplot(tempexpanded,[timeframestartdt[5],timeframeenddt[5]],os.path.abspath(os.path.join(subfolderpath,'tagsbyschool_1Y_'+country)),silent=pltsilent[idx])
        except Exception, err:
            print(err)
            pass

    plotnonetags=True
    if plotnonetags & ~Alloutdisable:
        nonetagplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],'issue',os.path.abspath(os.path.join(subfolderpath,'missingissue_1W_'+country)),silent=pltsilent[idx])
        nonetagplot(temptopconvdfcopy,[timeframestartdt[0],timeframeenddt[0]],'school',os.path.abspath(os.path.join(subfolderpath,'missingschool_1W_'+country)),silent=pltsilent[idx])    

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