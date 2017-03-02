# -*- coding: utf-8 -*-
"""
Created on Thu Mar 02 14:05:46 2017
Data modification functions
@author: Owner
"""
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re

def splitdatetime(dataframe,attrlist,inplace=True):
    """Scan through dataframe to find items in attrlist to be split into two new columns of Date and Time     
    """          
    if type(attrlist)==str:
        attrlist=[attrlist]
    if not inplace:
        outputdf=pd.DataFrame()        
    for item in attrlist:
        if hasattr(dataframe, item):   
            temp = pd.DatetimeIndex(dataframe[item])
            if inplace:
                dataframe[item+'_Date'] = temp.date
                dataframe[item+'_Date'] = pd.to_datetime(dataframe[item+'_Date'])
                dataframe[item+'_Time'] = temp.time
            else:
                outputdf[item+'_Date'] = temp.date
                outputdf[item+'_Date'] = pd.to_datetime(dataframe[item+'_Date'])
                outputdf[item+'_Time'] = temp.time
    if not inplace:
        return outputdf            
            
#need to return new dataframe to be merged back. current implementation is terrible               
#%%
def getadminname(s,admindf):
    """Convert adminname from id_num to actual str name
    """
    extractednamelist=admindf[admindf.id==s].name.values
    if extractednamelist:
         adminname=extractednamelist[0]
    else:
         adminname=None
    return adminname

def changenonetostr(s,text='None'):
    """Convert Nonetype to string 'None' for grouping 
    """
    if not s:
         return text
    else:
         return s
        
def changenonetoNone(s):
    """Convert str 'None' to Nonetype
    """
    if s=='None':
        return None
    else:
        return s
        
def changenonetotimedeltazero(s):
    """Convert missing timedeltas to timedelta(0)
    """    
    if s=='None' or s is None: 
        return pd.Timedelta(0)
    else:
        return s
        
def changenattotimedeltazero(s):
    """Convert NaTType to timedelta(0)
    """
    if type(s)==pd.tslib.NaTType:
        return pd.Timedelta(0)
    else:
        return s

def parsingconvtext(retrievedtext,customtextlist):
    """Sanitize text by removing common text strings and that in customtextlist
    """
    if not retrievedtext: #in case empty text
        retrievedtext=changenonetostr(retrievedtext)
    newtext=BeautifulSoup(retrievedtext).get_text() 
        #newtext=changenonetostr(retrievedtext)
    #newtext=BeautifulSoup(newtext).get_text()  
    #remove http links
    newtext=re.sub(r'http\S+', '', newtext)
    newtext=re.sub(r'\r\r\r\n', ' ', newtext)
    #remove LL specific text
    if customtextlist:
        for i in customtextlist:
            newtext=re.sub(i, '', newtext)
    return newtext

def getkeytimestats(s,refconvdf):
    """Extract key time stats from reference conversation df. 
    Returns series with columns [first_response, first_closed, last_closed].
    """
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
    """Counts the number of parts and their occurances in the conversation.
    Returns series with columns [numclosed, nummessage, numassign, numnote, numopened]
    """
    #create empty series             
    numcount=pd.Series(dict(close=0,comment=0,assignment=0,note=0,open=0))
    #update using retrieved stats
    df=refconvdf[(refconvdf.convid==s)]
    numcount.update(df.part_type.value_counts())
    #Force name change
    numcount.rename({ 'close' : 'numclosed','comment':'nummessage','assignment':'numassign','note':'numnote','open':'numopened'},inplace=True)    
    return numcount
    
def getfirstmessage(s,refconvdf):
    """Gets the first message of the conversation"""
    return refconvdf[(refconvdf.convid==s) & (refconvdf.part_type=='initial')].body.iloc[0]
        
def gettotaltags(s,refconvdf):
    """Split str of tags into list with each tag as an element.
    """         
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
     
             
def getschool(s,refconvdf,schooltaglist):    
    """Extract school from tag list
    Returns 'None' if not in list.
    """        
    taglist=gettotaltags(s,refconvdf)     
    schoolname=list(set(schooltaglist.name.values).intersection(taglist))     
    if not schoolname:#check if empty     
         return 'None'
    else:
         return schoolname
                    
    #some conversation might be forward by admin through email and thus not suitable to check user for details.
    #KIV for further work      
          #userid=convdf[(convdf.convid==s)& (convdf.idx_conv==0)].author.values[0]
          #print('Missing schoolname - trying to get')
          #try: 
          #     schoolname=User.find(id=userid).custom_attributes['School Name']
          #     return schoolname
          #except KeyError:
          #     return None
                                                   
     #if numtags==0:# check user if empty tag list
     #     userid=convdf[(convdf.convid==s)& (convdf.idx_conv==0)].author.values[0]
     #     return User.find(id=userid).custom_attributes['School Name']
     #else:
     #     schoolname=list(set(schooltag.name.values).intersection(taglist))
     #     return schoolname
     
def getissue(s,refconvdf,issuetaglist):
    """Extract issue from tag list     
    Returns 'None' if not in list"""
     #check if empty
    taglist=gettotaltags(s,refconvdf)
    issuename=list(set(issuetaglist.name.values).intersection(taglist))     
    if not issuename:
         return 'None'
    else:
         return issuename

def countissue(s):
    """Count number of issues"""    
    if s:#check if Nonetype.
        if s=='None':
        #if type(s)==str or type(s)==float:#Handle 
            return 0
        else:
            return len(s)
    else:#if empty
        return 0                     

def bintime(s,tunit,timebin,nanval):
    """Bin times into bins defined by edges in timebin.
    Unit of timebin needs to be defined 'h','s',etc    
    Returns nanval for missing timedeltas
    """    
    #same functionality can make use of pandas.cut
    #but have to convert column to seconds and bin accordingly i.e:
    #pd.cut(test.dt.seconds,[i*3600 for i in [1,2,3,4,5]],labels=[1,2,3,4])
    #and use pd.fillna(lastbin) for stuff outside bin
    #.cat.set_categories([0,1,2,3,4]).fillna(0)
    #test2['s_response_bin']=pd.cut(test2.s_to_first_response.dt.seconds,[i*3600 for i in [0,1,2,3,4]],labels=[1,2,3,4]).cat.set_categories([0,1,2,3,4]).fillna(0)
    #50% faster, but can't deal with values greater than last bin. Workaround- use an extremely large number ['0-1','1-2', '2-3','3-4','4-12','12-24','>24','UN'] 
    '''
         tt.tic()         
         test4=pd.cut(test2.s_to_last_closed.dt.total_seconds(),[i*3600 for i in [0,1,2,3,4,12,24,365*24]],labels=['0-1','1-2', '2-3','3-4','4-12','12-24','>24']).cat.add_categories(['UN']).fillna('UN')
         
         tt.toc()
         
         #del test2['s_response_bin']
         tt.tic()
         test3=test2.s_to_last_closed.apply(lambda s: af.bintime(s,'h',resolvebinlist,0))
         tt.toc()
         test5=pd.DataFrame([test3,test4]).transpose()
    '''
    for i in timebin[0:-1]:
        if s == 'None' or type(s)==pd.tslib.NaTType:
            binval=nanval
            break
        if s <= np.timedelta64(i, tunit):#timeunits=s / np.timedelta64(1, unit)
            binval=i
            break
        else:             
            binval= timebin[-1]
    return binval        