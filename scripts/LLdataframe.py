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
#build each as a separate function?
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

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir)))
from configs import pid,key
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
dflist=['convdf','topconvdf','userdf']

rebuildconvdf=True
rebuildtopconvdf=True
rebuilduser=True
output=True
toplot=False

rebuild=[rebuildconvdf,rebuildtopconvdf,rebuilduser]
datetimeattrlist=['created_at','first_response','first_closed','last_closed','updated_at']
datetimeattrspltlist=['created_at_Date','created_at_Time']
timedeltaattrlist=['s_to_first_response','s_to_first_closed','s_to_last_closed','s_to_last_update']
#listlist=['issue','school']

#df.Col3 = df.Col3.apply(literal_eval)
#import os.path
#load the files if rebuild is off #coerce may cause potential bugs !!!!!!!!!!!!!!
for i,nfile in enumerate(filelist):
    if os.path.isfile(nfile):
        if not rebuild[i]:            
            exec("%s = pd.read_csv(\'%s\',sep='\t', encoding='utf-8')" %(dflist[i],filelist[i]))
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
issuename=['Admin','Apps','Attendance','Bug','Bulletins','Check In/Out','Checklist','Contact Sales Support','Feedback','Fees','Forward to Malaysia Team','Forward to School','Integration Issue',
           'Internal:SPAM','LFR','Login Help','Logs','Moments','Notifications','Other Issue Type','Portfolio','Promotion','Wrong Parent Particulars','Weekly Digest','Change of Particulars','User Guide'
           'Duplicate','Wrong Recipient','Security Alert (Google)','General Enquiry']
issuetag=tagdf[tagdf.name.isin(issuename)]

#group tags by school
schooltag=tagdf[~tagdf.name.isin(issuename)]
                
print('Retrieved Issuetag and Schooltag Df from Intercom')
#%% Get Users ##########too large. need scrolling api
#loading from csv may not give recent info. need to pull from intercom for latest
from intercom import User
userdatetimeattrlist=['created_at','last_request_at','remote_created_at','signed_up_at','updated_at']
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
if rebuild[1]:
     print ('Getting all Conversations from Intercom')
     tempdictlist=[]
     itercounter=1
     for item in Conversation.find_all():
         tempdictlist.append(item.to_dict.copy())
         
         try:        
             if itercounter%(250)==0:#display progress counter
                 print('Retrieved ' + str(itercounter) + ' conversations')                     
         except ZeroDivisionError: 
             pass  
         itercounter+=1
     
     totalconv=len(tempdictlist)
     print('Retrieved ' + str(totalconv) + ' conversations')                     
     
     topconvdf=pd.DataFrame(tempdictlist) 
     
     #convert id
     topconvdf.assignee=topconvdf.assignee.apply(lambda s: s.id)
     topconvdf['adminname']=topconvdf.assignee.apply(lambda s: getadminname(s,admindf))
     topconvdf.user=topconvdf.user.apply(lambda s: s.id)
              
     itercounter=1
     missinguserdf=0     
     df=[]
     for index, row in topconvdf.iterrows():          
          try:        
               if itercounter%(int(totalconv/5))==0:#display progress counter
                      print('Processed ' + str(itercounter)+'/'+str(totalconv) + ' conversations')                     
          except ZeroDivisionError: 
               pass  
                    
          userid=row.user
          idxdf=userdf['id']==userid#count number of occurance
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
          else:
               userdetails=userdf[userdf['id']==userid].iloc[0]#.to_dict()#found in df, extract values out
               #userdetails=userdetails[userdetails.keys()[0]]#format may be different. possible source of errors!!!!!!!!!!!!!!!! keys used because method returns a series
          
          df.append(dict(username=userdetails.get('name'),email=userdetails.get('email'),role=userdetails.get('role')))                                                            
          itercounter+=1
          #df=pd.Series([dict(username=userdetails.get('name'),email=userdetails.get('email'),role=userdetails.get('role'))])
     topconvdf=topconvdf.merge(pd.DataFrame(df),left_index=True, right_index=True)
          
     print('Extracted all conversations')              
     print('Found #' + str(missinguserdf)+ ' missing users')
     print('Updated userdf')
     
     #rename so that it doesn't conflict when pulling conversation parts
     topconvdf=topconvdf.rename(columns={ 'id' : 'convid'})
     #convert columns with datetime strings to datetime objects
     topconvdf['updated_at']=pd.to_datetime(topconvdf['updated_at'],unit='s')
     topconvdf['created_at']=pd.to_datetime(topconvdf['created_at'],unit='s')
     #split datetime for created_at into two parts so that can do comparison for time binning
     splitdatetime(topconvdf,datetimeattrlist[0])          
     
else:
     print ('Load All conversations from csv')     
     totalconv=len(topconvdf.index)     
print('Total Conversations: ' + str(totalconv))
print('Time started: '+ str(datetime.datetime.now()))               
               
 
#%% create another dataframe with all conversation parts
tt.tic()
attrnames=['author','created_at','body','id','notified_at','part_type','assigned_to','url','attachments','subject']

conv=[]
if rebuild[0]:
     print('Retrieving full content of conversations from Intercom')
     itercounter=1     
     
     for convid in topconvdf.convid:
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
               #conv_part['created_at']=datetime.datetime.fromtimestamp(conv_part['created_at'])
               #conv_part['notified_at']=datetime.datetime.fromtimestamp(conv_part['notified_at'])
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
               
               #append to final list  
               conv.append(conv_part)
               #Just in case the constant request trigger api limit
               if Intercom.rate_limit_details['remaining']<25:
                      print('Current rate: %d. Sleeping for 1 min' %Intercom.rate_limit_details['remaining'])
                      time.sleep(60)           
                      print('Resuming.....')
                      
          itercounter+=1
          
     convdf=pd.DataFrame(conv)
     print('Built convdf')
     
     #convert possible datetime strings to datetime objects
     convdf['notified_at']=pd.to_datetime(convdf['notified_at'],unit='s')
     #convdf['created_at']=pd.to_datetime(convdf['created_at'],unit='s')
     #split datetime into two parts so that can do comparison for time binning
     splitdatetime(convdf,datetimeattrlist)
else:
     print ('Loaded Conversations from csv')   

tt.toc()

#%% Calculate values to update adminconvdf
#Find response time 
def getfirstresponse(s):
    return convdf[(convdf.convid==s) & (convdf.idx_conv==1)].created_at.values[0]

## to combine into one to avoid constant transversing
def getnumclosed(s):#find number of closed within conversation
    return len(convdf[(convdf.convid==s) & (convdf.part_type=='close')])
    
def getnumopened(s):#find number of closed within conversation
    return len(convdf[(convdf.convid==s) & (convdf.part_type=='open')])    
    
def getnumcomments(s):
    return len(convdf[(convdf.convid==s) & (convdf.part_type=='comment')])    
    
def getnumnotes(s):
    return len(convdf[(convdf.convid==s) & (convdf.part_type=='note')])    

def getnumassignments(s):
    return len(convdf[(convdf.convid==s) & (convdf.part_type=='assignment')])        
        
#Find first closed time
def getfirstclosed(s):
    if getnumclosed(s)>0:
         return convdf[(convdf.convid==s) & (convdf.part_type=='close')].head(1).created_at.values[0]
    else: 
         return None
#last closed time          
def getlastclosed(s):
    if getnumclosed(s)>0:
         return convdf[(convdf.convid==s) & (convdf.part_type=='close')].tail(1).created_at.values[0]
    else: 
         return None
         
def gettotaltags(s):         
     taglist=[]     
     for ptag in convdf[(convdf.convid==s)].tags.values:
          if type(ptag)==str or type(ptag)==unicode:
               ptag=ptag[1:-1].split(', ')        #possible source of error.. added space to cater for reading in csv.       
          if ptag:
               try: 
                    for ele in ptag:
                         taglist.append(ele)     
               except TypeError:
                    pass
     return taglist
     
             
def getschool(s):
     #some conversation might be forward by admin through email and thus not suitable to check user for details.
     #check if empty     
     taglist=gettotaltags(s)     
     schoolname=list(set(schooltag.name.values).intersection(taglist))
     if not schoolname:
          return None
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
def getissue(s):     
     #check if empty
     taglist=gettotaltags(s)
     issuename=list(set(issuetag.name.values).intersection(taglist))     
     if not issuename:
          return None
     else:
          return issuename

def countissue(s):    #assuming issues are in list
    if s:
        if type(s)==str:
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
    if s=='None':
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

def recogtf(s,timebin):
    tfstr=['Week','Month','6 Months','Year']    
    binout=bintime(pd.Timedelta(s),'D',timebin,0)
    binoutidx=[i for i,x in enumerate(timeframe) if x==binout]    
    return tfstr[binoutidx[0]]

if rebuild[1]:
     print('Building additional info for each conversation')
     #response=[]
     topconvdfcopy=topconvdf.copy()#keep original so that don't mess
     
     generateddf=topconvdfcopy.convid.apply(lambda s: pd.Series({'first_response':getfirstresponse(s),
                                                                       'first_closed': getfirstclosed(s),
                                                                       'last_closed': getlastclosed(s),
                                                                       'numclosed': getnumclosed(s),
                                                                       'numopened': getnumopened(s),
                                                                       'nummessage': getnumcomments(s),
                                                                       'numnote': getnumnotes(s), 
                                                                       'numassign': getnumassignments(s),
                                                                       'numtags': len(gettotaltags(s)),                                                                  
                                                                       'issue': getissue(s),#duplicate
                                                                       'school': getschool(s),#duplicate
                                                                            }))
         
         #getschooldf=adminconvdfcopy.convid.apply(lambda s: pd.Series({'school': getschool(s)}))    
         
         #subtracting. not sure whats the shift() for
         #df['col'] - df['col'].shift()
     
     #get response,firstclose,lastclose timedelta
     tdeltadf=generateddf[['first_response', 'first_closed','last_closed']].sub(topconvdfcopy['created_at'], axis=0)
     tdeltadf.columns = ['s_to_first_response', 's_to_first_closed','s_to_last_closed']
     tudeltadf=topconvdfcopy[['updated_at']].sub(topconvdfcopy['created_at'], axis=0)
     tudeltadf.columns = ['s_to_last_update']
     
     topconvdfcopy=topconvdfcopy.merge(generateddf, left_index=True, right_index=True)
     topconvdfcopy=topconvdfcopy.merge(tdeltadf, left_index=True, right_index=True)
     topconvdfcopy=topconvdfcopy.merge(tudeltadf, left_index=True, right_index=True)
     #adminconvdfcopy.merge(getschooldf, left_index=True, right_index=True)
     print('Additional info for each conversation')     
     
     #change open to 1 for easier understanding
     topconvdfcopy['open']=topconvdfcopy.open.apply(lambda s: s*1)    
     #change none to string so that can group
     topconvdfcopy['issue']=topconvdfcopy.issue.apply(lambda s: changenonetostr(s))
     #change none to string so that can group
     topconvdfcopy['school']=topconvdfcopy.school.apply(lambda s: changenonetostr(s))
     #count issues
     topconvdfcopy['numissues']=topconvdfcopy.issue.apply(lambda s: countissue(s))    
     
     #bintime for pivot tables
     topconvdfcopy['s_response_bin']=topconvdfcopy.s_to_first_response.apply(lambda s: bintime(s,'h',[1,2,3,4],0))
     #can't print if type is replaced with str None
     #Have to fill nattype with none first #this screws with plotly.
     topconvdfcopy['s_to_last_closed'] = topconvdfcopy.s_to_last_closed.apply(lambda x: x if isinstance(x, pd.tslib.Timedelta) 
                                           and not isinstance(x, pd.tslib.NaTType) else 'None')    
     topconvdfcopy['s_resolve_bin']=topconvdfcopy.s_to_last_closed.apply(lambda s: bintime(s,'h',[1,2,3,4],0))
     
else:
     topconvdfcopy=topconvdf.copy()
     if not hasattr(topconvdfcopy,'created_at_Date'):
         splitdatetime(topconvdfcopy,datetimeattrlist[0])
 
     #lists are read in as string. need to convert back so that can process. should move to common procedure.
     str2listdf=topconvdfcopy.convid.apply(lambda s: pd.Series({'issue': getissue(s),'school': getschool(s)}))     #duplicate
     #cheating abit here. instead of processing string within adminconvdfcopy, getting entire data from convdf
     del topconvdfcopy['issue']
     del topconvdfcopy['school']
     topconvdfcopy=topconvdfcopy.merge(str2listdf, left_index=True, right_index=True)
     
     print('Metrics loaded from csv')                      

#%%group by tf
timeframe=[7,30,180,365]
#timeframe=[7]
timeframedt=[timenow.date()-datetime.timedelta(dt) for dt in timeframe]
             
#change none to string so that can group
topconvdfcopy['issue']=topconvdfcopy.issue.apply(lambda s: changenonetostr(s))
#change none to string so that can group
topconvdfcopy['school']=topconvdfcopy.school.apply(lambda s: changenonetostr(s))

#add end of created day
topconvdfcopy['created_at_EOD']=topconvdfcopy.created_at_Date.apply(lambda s: s+pd.Timedelta('1 days')+pd.Timedelta('-1us'))

stats=[[[] for _ in range(9)] for _ in range(len(timeframe))]     
for i,tf in enumerate(timeframe):             
    #temptimedf=adminconvdfcopy.loc[adminconvdfcopy['created_at_Date'] >= i].copy()
    issuedf=topconvdfcopy.loc[topconvdfcopy['created_at_Date'] >= timeframedt[i]].copy()

    #handling multiple tags    
    oissuedf=issuedf[issuedf['numissues']==0]#collect rows with issues equal to 0    
    missuedf=issuedf[issuedf['numissues']>0]#collect rows with issues greater than 1
        
    #Build new df 
    df=[]
    for index, row in missuedf.iterrows():                   
        for issue in row.issue:            
            temprow=row.copy()#duplicate row
            temprow.issue=issue#replace multi issue of duplicated row with single issue
            df.append(temprow)
    missuedf=pd.DataFrame(df)            
    
    issuedf=oissuedf.append(missuedf)        #recombine
    issuedf=issuedf.sort_index() #sort
    
    #Handling schools    
    oschooldf=issuedf[issuedf['school']=='None']#collect rows with schools with none    
    mschooldf=issuedf[issuedf['school']!='None']#collect rows with schools 
    
    #Build new df 
    df=[]
    for index, row in mschooldf.iterrows():                           
        for school in row.school:            
            temprow=row.copy()#duplicate row
            temprow.school=school#replace multi sch of duplicated row with single sch
            df.append(temprow)
    mschooldf=pd.DataFrame(df)            
    
    issuedf=oschooldf.append(mschooldf)        #recombine
    issuedf=issuedf.sort_index() #sort
    
    groupbyissue=issuedf.groupby('issue')   
    #groupbyissuestats=groupbyissue.describe()
    
    #groupbyschool=issuedf.groupby('school')   
    groupbyschool=issuedf[['issue','school']]
    groupbyschool=groupbyschool.pivot_table(index='issue', columns='school', aggfunc=len, fill_value=0)
    #sumoftags=pd.DataFrame(groupbyschool.transpose().sum())
    #groupbyschool['Total']=sumoftags
   
    #groupbyschoolstats=groupbyschool.describe()
    
    #generate data for excel csv output
    responsepivotdf=issuedf[['issue','s_response_bin']]
    responsepivotdf=responsepivotdf.pivot_table(index='issue', columns='s_response_bin', aggfunc=len, fill_value=0)
    sumoftags=pd.DataFrame(responsepivotdf.transpose().sum())
    responsepivotdf['Total']=sumoftags

    resolvepivotdf=issuedf[['issue','s_resolve_bin']]    
    resolvepivotdf=resolvepivotdf.pivot_table(index='issue', columns='s_resolve_bin', aggfunc=len, fill_value=0)
    sumoftags=pd.DataFrame(resolvepivotdf.transpose().sum())
    resolvepivotdf['Total']=sumoftags
    
    #number of conversations split by tag x-axis=day
    #bug: days with no tags will not show up.
    tagpivotdf=issuedf[['issue','created_at_Date']]        
    tagpivotdf=tagpivotdf.pivot_table(index='issue', columns='created_at_Date', aggfunc=len, fill_value=0)
    sumoftags=pd.DataFrame(tagpivotdf.transpose().sum())#appending this in will cause the column index to become a object, making appending via columns screwy.
    

    tagRpivotdf=issuedf[['s_to_first_response','created_at_Date']]    
    tagRpivotdfdes=tagRpivotdf.groupby('created_at_Date').describe()
    tagRpivotdfs=tagRpivotdfdes.unstack().loc[:,(slice(None),['mean','max'])]
    tagRpivotdfs=tagRpivotdfs['s_to_first_response'].transpose()
    
    tagpivotdf=tagpivotdf.append(tagRpivotdfs)
    tagpivotdf=tagpivotdf.assign(Total=sumoftags)
    
    #number of conversations split by admin x-axis=admin
    adminpivotdf=issuedf[['issue','adminname']]    
    adminpivotdf=adminpivotdf.pivot_table(index='issue', columns='adminname', aggfunc=len, fill_value=0)
    sumoftags=pd.DataFrame(adminpivotdf.transpose().sum())    

    adminRpivotdf=issuedf[['s_to_first_response','adminname']]    
    adminRpivotdfdes=adminRpivotdf.groupby('adminname').describe()
    adminRpivotdfs=adminRpivotdfdes.unstack().loc[:,(slice(None),['mean','max'])]
    adminRpivotdfs=adminRpivotdfs['s_to_first_response'].transpose()
    
    adminpivotdf=adminpivotdf.append(adminRpivotdfs)
    adminpivotdf['Total']=sumoftags
    
    #time to respond and resolve (for all). Duplicate. to re-write because i've screwed with the original df at top.
    overallconvdf=topconvdfcopy.loc[topconvdfcopy['created_at_Date'] >= timeframedt[i]].copy()    
    
    #add those still open but created before date.
    openbeforetf=topconvdfcopy.loc[topconvdfcopy['created_at_Date'] < timeframedt[i]].copy()
    openbeforetf=openbeforetf.loc[openbeforetf['open'] ==1]
    
    opentagconvdf=overallconvdf.append(openbeforetf)
    
    #collect rows with first_closed = none 
    #openconvdf_none=overallconvdf[overallconvdf['first_closed'].isnull()].copy()
    #openconvdf_none=issuedf[issuedf['first_closed'].isnull()].copy()
    
    #collect rows with first_closed >= end of day     
    #problem seems to be just here. splitdatetime turns value into an object. the addition doesn't work
    #endofday=issuedf['created_at_Date']+pd.Timedelta('1 days')+pd.Timedelta('-1us') 
    
    #endofday=pd.to_datetime(issuedf['created_at_Date'])+pd.Timedelta('1 days')+pd.Timedelta('-1us') #use pd.to_datetime to change
    
    #openconvdf_stillopen=issuedf[issuedf['first_closed'] >= endofday].copy()    
    
    #if negative value means issue was closed before end of day. safe
    eodtdelta=opentagconvdf.first_closed.sub(opentagconvdf.created_at_EOD,axis=0)#<---------------usage of first closed misses conversations that have multiple close/openings.
    #forced into 0 bin.
        
    eodbintf=range(1,1+timeframe[i])
    eodbin=eodtdelta.apply(lambda s: bintime(s,'D',eodbintf,pd.NaT))
    eodbin[eodbin.isnull()]=(datetime.datetime.now().date()-opentagconvdf.created_at_Date[eodbin.isnull()]).astype('timedelta64[D]')    
    openconvdf=opentagconvdf.assign(eodbin=eodbin.astype(int))
    
    opentagpivotdf=openconvdf[['adminname','created_at_Date','eodbin']].copy()    
    #augment df with duplicates
    #convert those with None to max based on current date
    
    datetime.datetime.now().date()
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
    
    opentagpivotdfsubset=opentagpivotdf[opentagpivotdf['created_at_Date']>= timeframedt[i]] #hide all those outside timeframe
    opentagpivotdf=opentagpivotdfsubset[['adminname','created_at_Date']].pivot_table(index='adminname', columns='created_at_Date', aggfunc=len, fill_value=0)    
    sumoftags=pd.DataFrame(opentagpivotdf.transpose().sum())        
    

    #opentagRpivotdf=openconvdf[['s_to_first_response','created_at_Date']].copy()    
    #opentagRpivotdfdes=opentagRpivotdf.groupby('created_at_Date').describe()
    #opentagRpivotdfs=opentagRpivotdfdes.unstack().loc[:,(slice(None),['mean','max'])]
    #opentagRpivotdfs=opentagRpivotdfs['s_to_first_response'].transpose()

    #opentagpivotdf=opentagpivotdf.append(opentagRpivotdfs)    
    
    opentagRpivotdfdes2=opentagpivotdfsubset.groupby('created_at_Date').describe()
    opentagRpivotdfs2=opentagRpivotdfdes2.unstack().loc[:,(slice(None),['count'])]
    opentagRpivotdfs2=opentagRpivotdfs2['eodbin'].transpose()
    opentagpivotdf=opentagpivotdf.append(opentagRpivotdfs2)    
    opentagpivotdf['Total']=sumoftags
        
    #sort to list    
    stats[i][0]=responsepivotdf
    stats[i][1]=resolvepivotdf
    stats[i][2]=groupbyissue
    stats[i][3]=groupbyschool
    stats[i][4]=tagpivotdf    
    stats[i][5]=adminpivotdf
    stats[i][6]=opentagpivotdf
    stats[i][7]=overallconvdf
    stats[i][8]=dict(start=timeframedt[i],end=timenow.date(),numconversations=len(overallconvdf),tf=recogtf(timenow.date()-timeframedt[i],timeframe))
    
#for key, item in issuestats[1][0]:
#    print issuestats[1][0].get_group(key), "\n\n"
#df[df['BoolCol'] == True].index.tolist()
#[[x,l.count(x)] for x in set(listofissues)]
#past week

outputstats=True
if outputstats:
    response_csv_path=os.path.abspath(os.path.join(outputfolder,'response_csv.csv'))
    with open(response_csv_path, 'w') as f:
        stats[0][0].to_csv(f,sep='\t')
    resolve_csv_path=os.path.abspath(os.path.join(outputfolder,'resolve_csv.csv'))        
    with open(resolve_csv_path, 'w') as f:
        stats[0][1].to_csv(f,sep='\t')        
    dailytagcount_csv_path=os.path.abspath(os.path.join(outputfolder,'dailytagcount_csv.csv'))            
    with open(dailytagcount_csv_path, 'w') as f: 
        stats[0][4].to_csv(f,sep='\t')        

#%% Tags by timeframe
def tagsbytfplot(inputpivtable,ofilename):    
    pivtable=inputpivtable[4].copy()#for the week        
    plottf=inputpivtable[-1]['tf']    
    start=inputpivtable[-1]['start']
    end=inputpivtable[-1]['end']
    n=inputpivtable[-1]['numconversations']    
    day_piv=pivtable.ix[:-2,:-1]
    aggstats_piv=pivtable.ix[-2:,:-1].copy()
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
        
    layout = Layout(title='Conversations (n = '+ str(n) +') for last '+ plottf + ' ( '+str(start)+' - '+str(end)+' )',
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

    #%% Tags by timeframe
def tagsbyschoolplot(inputpivtable,ofilename):    
    pivtable=inputpivtable[3].copy()        
    plottf=inputpivtable[-1]['tf']    
    start=inputpivtable[-1]['start']
    end=inputpivtable[-1]['end']
    n=inputpivtable[-1]['numconversations']    
    day_piv=pivtable    
        
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)        
        
    layout = Layout(title='Conversation Tags by School (n = '+ str(n) +') for last '+ plottf + ' ( '+str(start)+' - '+str(end)+' )',
                    yaxis=dict(title='Tags'),
                    xaxis=dict(title='School'),
                    barmode='relative'                    
                    )
    fig = dict(data=data_piv, layout=layout )
    plot(fig,filename=ofilename)
    
#%% Open conversations by day
def openconvobytfplot(inputpivtable,ofilename):
    pivtable=inputpivtable[6].copy()#for the week
    plottf=inputpivtable[-1]['tf']
    start=inputpivtable[-1]['start']    
    end=inputpivtable[-1]['end']
    day_piv=pivtable.ix[:-1,:-1]
    convocount=pivtable.ix[-1,:-1]
    
    data_piv=[]    
    for idx,row in day_piv.iterrows():
        tempdata_piv = Bar(x=day_piv.columns, y=row.values, name=idx)
        data_piv.append(tempdata_piv)
           
    layout = Layout(title='Conversations still open at the end of day for past '+ plottf + ' ( '+str(start)+' - '+str(end)+' )',
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
    
#%%
def allconvobyadminplot(inputpivtable,ofilename):
    pivtable=inputpivtable[5].copy()#for the week
    plottf=inputpivtable[-1]['tf']
    start=inputpivtable[-1]['start']
    end=inputpivtable[-1]['end']
    n=inputpivtable[-1]['numconversations']    
    day_piv=pivtable.ix[:-2,:-1]
    aggstats_piv=pivtable.ix[-2:,:-1].copy()
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
        
    layout = Layout(title='Conversations (n = '+ str(n) +') for last '+ plottf + ' ( '+str(start)+' - '+str(end)+' )',
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
    
#%%
def overalltagplot(inputpivtable,ofilename):
    pivtable=inputpivtable[4].copy()#for the month
    plottf=inputpivtable[-1]['tf']
    start=inputpivtable[-1]['start']
    end=inputpivtable[-1]['end']
    n=inputpivtable[-1]['numconversations']    
    #day_piv=pivtable.ix[:-2,:-1]
    overall_piv=pivtable['Total'][:-2]   
    x=overall_piv.index.tolist()
    y=overall_piv.tolist() 
    datao_piv=[Bar(x=x, y=y)]
    layout = Layout(title='Total conversations (n = '+ str(n) +') split by tags for the last '+ plottf + ' ( '+str(start)+' - '+str(end)+' )',
                    yaxis=dict(title='Number'),
                    xaxis=dict(title='Tags'),
                    annotations=[   dict(x=xi,y=yi, text=str(yi),
                                    xanchor='center', yanchor='bottom',
                                    showarrow=False) for xi, yi in zip(x, y)]
                    )
    fig = dict(data=datao_piv, layout=layout )
    plot(fig,filename=ofilename)
#%%
def overalltagplot2(inputpivtablelist,ofilename):
    datalist=[]
    nlist=[]

    for idx,inputpivtable in enumerate(inputpivtablelist):
        pivtable=inputpivtable[4].copy()#for the month
        plottf=inputpivtable[-1]['tf']
        start=inputpivtable[-1]['start']
        end=inputpivtable[-1]['end']
        n=inputpivtable[-1]['numconversations']    
        #day_piv=pivtable.ix[:-2,:-1]
        overall_piv=pivtable['Total'][:-2]   
        x=overall_piv.index.tolist()
        y=overall_piv.tolist()
        datao_piv=Bar(x=x, y=y,name=plottf+' ( '+str(start)+' - '+str(end)+' )')
        
        datalist.append(datao_piv)
        nlist.append(str(n))
        
    opstr='(' + ','.join(nlist) + ')'
        
    layout = Layout(title='Total conversations split by tags for past two '+ plottf + ', n = ' + opstr,
                    yaxis=dict(title='Number'),
                    xaxis=dict(title='Tags'),
                    barmode='group'
                    )
    fig = dict(data=datalist, layout=layout )
    plot(fig,filename=ofilename)    
#%% overallresponse
def overallresponsestatplot(inputdf,ofilename):
    responsestats=inputdf[7].copy()#past month
    plottf=inputdf[-1]['tf']
    start=inputdf[-1]['start']
    end=inputdf[-1]['end']
    responsestats=responsestats.sort_values('created_at',ascending=True)
    #convert 'None' str into None #this is a fucking terrible bandaid. Please fix soon
    responsestats['s_to_last_closed']=responsestats.s_to_last_closed.apply(lambda s: changenonetotimedeltazero(s))
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
        textstr='<br>'.join([nummessagestr,issuestr,schoolstr,usernamestr,adminnamestr,emailstr,numnotestr,numassignstr,numclosedstr])
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

    layout = Layout(    title='Overall response for last ' + plottf + ' ( '+str(start)+' - '+str(end)+' )',
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
    

#%% Plotting

plotallconvobyadmin=True
if plotallconvobyadmin:    
    allconvobyadminplot(stats[0],os.path.abspath(os.path.join(outputfolder,'tagsbyAdmin.html')))    

plotoveralltags=True
if plotoveralltags:
    overalltagplot(stats[1],os.path.abspath(os.path.join(outputfolder,'Overalltagsformonth.html')))
    overalltagplot(stats[0],os.path.abspath(os.path.join(outputfolder,'Overalltagsforweek.html')))
    
plotopenconvobytf=True    
if plotopenconvobytf:
    openconvobytfplot(stats[0],os.path.abspath(os.path.join(outputfolder,'openbyday_1W.html')))   
    openconvobytfplot(stats[3],os.path.abspath(os.path.join(outputfolder,'openbyday_1Y.html')))    
    
plottagsbyday=True
if plottagsbyday:
    tagsbytfplot(stats[0],os.path.abspath(os.path.join(outputfolder,'tagsbyday.html')))
            
plotoverallresponsestats=True
if plotoverallresponsestats:
    overallresponsestatplot(stats[0],os.path.abspath(os.path.join(outputfolder,'overallresponse_1W.html'))) 
    overallresponsestatplot(stats[1],os.path.abspath(os.path.join(outputfolder,'overallresponse_1M.html')))     
    #overallresponsestatplot(stats[2][7],'overallresponse_6M.html','6 Months')
    #overallresponsestatplot(stats[3][7],'overallresponse_1Y.html','Year') 

plottagsbyschool=True
if plottagsbyschool:
    tagsbyschoolplot(stats[0],os.path.abspath(os.path.join(outputfolder,'tagsbyschool.html')))
    tagsbyschoolplot(stats[1],os.path.abspath(os.path.join(outputfolder,'tagsbyschool.html')))
    
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
     if rebuild[0]:
          del convdfcopy['body']
          del convdfcopy['subject']
          del convdfcopy['attachments']
              
     convdfcopy.to_csv(convstatsf, sep='\t', encoding="utf-8")
     print('Written to '+convstatsf)
     
     #rearranging columns before output
     convcolumns=['adminname','convid','open','read','created_at','created_at_Date',
                       'created_at_Time','first_response','s_to_first_response','numclosed',
                       'first_closed','s_to_first_closed','last_closed','s_to_last_closed',
                       'updated_at','s_to_last_update','issue','numissues','school',
                       'numtags','nummessage','numassign','numclosed','numnote','user',
                       'username','email','role','assignee','s_response_bin',
                       's_resolve_bin']
                       
     topconvdfcopyoutput=topconvdfcopy.copy()                 
     if rebuild[1]: 
          del topconvdfcopy['conversation_message']
     topconvdfcopyoutput=topconvdfcopy[convcolumns]
     topconvdfcopyoutput.to_csv(topconvstatsf, sep='\t', encoding="utf-8")
     print('Written to '+ topconvstatsf)
     
     if rebuild[2]:
         #need to drop duplicates. ##########potential error source
         userdf.drop_duplicates('id').to_csv(userf, sep='\t', encoding="utf-8")
     print('Written to '+ userf)
          
    
     '''
     groupedbyadminstats.to_csv(groupbyadmintatsf,sep='\t', encoding="utf-8")
     groupedbyadmindatesummary.to_csv('summary.csv',sep='\t', encoding="utf-8")
     '''