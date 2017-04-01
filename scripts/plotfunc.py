# -*- coding: utf-8 -*-
"""
Created on Thu Mar 02 13:56:53 2017
functions for plotting
@author: Boon
"""
import pandas as pd
import datetime
import numpy as np
import augfunc as af
#import xlsxwriter

from plotly.offline import download_plotlyjs, plot
from plotly.graph_objs import Bar, Layout, Scatter, Pie 

#https://www.littlelives.com/img/identity/logo_littlelives_full_med.png

#%% General functions
def slicebytimeinterval(df,timeinterval,column='created_at_Date'):
    if timeinterval[0]>timeinterval[1]:
        print('Warning: timestart > timeend') 
    if not column=='created_at_Time':
        sliceddf=df[(df[column] >= pd.to_datetime(timeinterval[0])) & (df[column] < pd.to_datetime(timeinterval[1]))]
    else:
        sliceddf=df[(df[column] >= timeinterval[0]) & (df[column] < timeinterval[1])]
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

def recogtf(tf,timebin):#for printing timeframe in context
    timeframe=[7,30,180,365]#in days
    tfstr=['Week','Month','6 Months','Year']    
    binout=af.bintime(pd.Timedelta(tf),'D',timebin,0)
    binoutidx=[i for i,x in enumerate(timeframe) if x==binout]    
    return tfstr[binoutidx[0]],timeframe[binoutidx[0]]

      
    
#%% response and resolve pivottables for excel csv
def generatetagpivtbl(inputdf,columnname, timeinterval,forcecolumns=None):
    #responsepivotdf=generatetagpivtbl(issueschoolexpandeddf,'s_response_bin',[timeframestartdt[0],timeframeenddt[0]])
    #resolvepivotdf=generatetagpivtbl(issueschoolexpandeddf,'s_resolve_bin',[timeframestartdt[0],timeframeenddt[0]])    

    sliceddf=slicebytimeinterval(inputdf,timeinterval)
    if sliceddf.empty:
        raise ValueError('Empty sliceddf')
    numconversations=len(sliceddf.convid.unique())
    
    workindf=sliceddf[['issue',columnname]]
    pivtable=workindf.pivot_table(index='issue', columns=columnname, aggfunc=len, fill_value=0)
        
    sumoftags=pd.DataFrame(pivtable.transpose().sum())    
    pivtable['Total']=sumoftags    
    sumoftagsbycolumn=pd.DataFrame(pivtable.sum(),columns=['Grand Total'])
    pivtable=pivtable.append(sumoftagsbycolumn.transpose())
    
    if forcecolumns:
        for colname in forcecolumns:
            if colname not in pivtable.columns.values:
                pivtable[colname]=0
        #pivtable.sort_index(axis=1,inplace=True)
        pivtable=pivtable[forcecolumns+['Total']]
                                
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
def generateopentagpivdf(rawinputdf, timeinterval,timescriptstart=datetime.datetime.now()): #use only sliced, not the augmented one
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
    df.loc[df['last_closed'].isnull(), 'last_closed'] = timescriptstart#+pd.timedelta(1,'D')
        
    #get all conversations closed before interval
    closedbefore=slicebytimeinterval(df,[pd.to_datetime(0).date(), timeinterval[0]],'last_closed')
    #get all conversations open after interval
    openafter=slicebytimeinterval(df,[timeinterval[1],pd.to_datetime(timescriptstart).date()],'created_at')
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
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 
    
    responsestats=slicebytimeinterval(rawinputdf,timeinterval).copy()#overallconvdf
    responsestats=responsestats.sort_values('created_at',ascending=True)
    #convert 'None' str into None #this is a fucking terrible bandaid. Please fix soon
    #responsestats['s_to_first_response']=responsestats.s_to_first_response.apply(lambda s: changenonetotimedeltazero(s))
    responsestats['s_to_last_closed']=responsestats.s_to_last_closed.apply(lambda s: af.changenonetotimedeltazero(s))
    #responsestats['s_to_last_closed']=responsestats.s_to_last_closed.apply(lambda s: changenattotimedeltazero(s))
    responsestats['s_to_first_closed']=responsestats.s_to_first_closed.apply(lambda s: af.changenattotimedeltazero(s))
    responsestats['s_to_last_update']=responsestats.s_to_last_update.apply(lambda s: af.changenonetotimedeltazero(s))
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
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 
    
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
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 

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
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 

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
        plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 

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
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 
    
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
    
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1))    
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
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 
    #in case not assigned yet, adminname will be empty
    #inputdf.adminname=inputdf.adminname.apply(lambda s: changenonetostr(s,'Unassigned'))
    #inputdf.adminname.fillna('Unassigned',inplace=True)
    #inputdf.school=inputdf.school.apply(lambda s: changenonetostr(s))
    pivtable, notag, numconversations = getnonetags(inputdf, timeinterval, columnname)

    #check if empty, exit if true
    if notag.empty:
        print ('No missing tags for '+ columnname + ' found')
        return
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

#%% AGP generation
#change 4 donuts prev to line/bar chart with <4 and >4 only
def agpgen(inputdf, timeinterval,ofilename,responsecolumnlabels,resolvecolumnlabels):
    #troublshooting
    '''
    idx=0
    tempexpanded=issueschoolexpandeddf[(issueschoolexpandeddf.adminname.isin(admindfbycountry[idx].name))|(issueschoolexpandeddf.adminname.isnull())] 
    temptopconvdfcopy=topconvdfcopyutc[(topconvdfcopyutc.adminname.isin(admindfbycountry[idx].name))|(topconvdfcopyutc.adminname.isnull())] 
    inputdf=tempexpanded.copy()
    timeinterval=[timeframestartdt[0],timeframeenddt[0]]
    #prevtimeinterval=[timeframestartdt[1],timeframeenddt[1]]
    ofilename=os.path.abspath(os.path.join(subfolderpath,'Weeklyemail.xlsx'))    
    from plotfunc import *
    df=dftoprocess[0]
    
    '''
    tfstart=timeinterval[0]
    tfend=timeinterval[1]
    tfdelta=tfend-tfstart
    plottf,plottfn=recogtf(tfdelta,range(tfdelta.days+1)) 
    #generate list for initial timeframe and previous 4 timeframes
    tfdeltalist=[(plottfn+1)*i for i in [i for i in range(5)]]
    tflist=[[tf- pd.Timedelta(str(td)+" days") for tf in timeinterval] for td in tfdeltalist]
        
    #split weekday from weekend
    weekdaynumdf=inputdf.created_at_Date.apply(lambda s: s.weekday())
    weekdaydf=inputdf[weekdaynumdf<5]
    weekenddf=inputdf[weekdaynumdf>=5]
    
    #working hours 0830-1800
    workinghourdf=slicebytimeinterval(weekdaydf,[datetime.time(8,30),datetime.time(18,0)],column='created_at_Time')
    afterworkinghourdf=weekdaydf[~weekdaydf.convid.isin(workinghourdf.convid)]
    
    dftoprocess=[inputdf,workinghourdf,afterworkinghourdf,weekenddf]
    dfnametoprocess=['Overall','Weekday During Office Hours','Weekday After Office Hours','Weekend']
    
    #poor implementation. need to fix!
    #responsebinlist=[1,2,3,4,5]
    #resolvebinlist=[1,2,3,4,12,24,25]
    #responsecolumnlabels=['0-1','1-2','2-3','3-4','>4']
    #resolvecolumnlabels=['0-1','1-2', '2-3','3-4','4-12','12-24','>24','UN']
    
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(ofilename, engine='xlsxwriter')
    workbook  = writer.book   
    
    merge_format = workbook.add_format({
                                                #'bold':     True,
                                                #'border':   6,
                                                'align':    'center',
                                                'valign':   'vcenter'#,
                                                #'fg_color': '#D7E4BC',
                                            })    
    
    for idx,df in enumerate(dftoprocess):
        tosplit=((idx==0)|(idx==1))        
        if tosplit:
            responsetoprocess=[0,1,2,3,4]
        else:
            responsetoprocess=[0,1]
        
        responsepivotdf=[[]for i in xrange(len(responsetoprocess))]
        sliceddf_resp=[[]for i in xrange(len(responsetoprocess))]
        uniqueconv_resp=[[]for i in xrange(len(responsetoprocess))]
        within4hours_resp=[[]for i in xrange(len(responsetoprocess))]
        over4hours_resp=[[]for i in xrange(len(responsetoprocess))]
                        
        try:                        
            for tfidx in responsetoprocess:
                sliceddf_resp[tfidx], responsepivotdf[tfidx],uniqueconv_resp[tfidx]=generatetagpivtbl(df,'s_response_bin',tflist[tfidx],responsecolumnlabels)            
                within4hours_resp[tfidx]=responsepivotdf[tfidx].iloc[-1][0:4].sum()#if timebins change this will go haywire!!!!!!!!
                over4hours_resp[tfidx]=responsepivotdf[tfidx].iloc[-1][4:-1].sum()#if timebins change this will go haywire!!!!!!!!
                
                if tfidx==0:
                    responsepivotdf[tfidx]['%']=responsepivotdf[tfidx]['Total'].apply(lambda s: float(s)/responsepivotdf[tfidx]['Total'][-1]*100)#get percentage of total for printing
            
            sliceddf_resolv, resolvepivotdf,uniqueconv_resolv=generatetagpivtbl(df,'s_resolve_bin',tflist[0],resolvecolumnlabels)  
            tagpivotdf,responsestats,allconvnum=generatetagpivdf(df,'created_at_Date',tflist[0])
        except ValueError:
            continue
        
        
        #modify the results to look like AGP. possibly want to shift it to within function?

        #except KeyError:
        #    within4hours=None
        try:
             unresolvedthisweek=resolvepivotdf[resolvecolumnlabels[-1]]['Grand Total']
        except KeyError:
             unresolvedthisweek=0
             print ('No unresolved found')
                       
        uniquedunresolved=len(sliceddf_resolv[sliceddf_resolv['s_resolve_bin']=='UN'].convid.unique())
        
        #try:
        #    within4hours=float(responsepivotdf['Total'][-1]-responsepivotdf[5][-1])/totalconvthisweek*100
        
        #rename so that column labels make sense 
        #responsepivotdf.rename(columns={1: '0-1', 2: '1-2', 3:'2-3',4:'3-4',5:'>4','Total':'Grand Total',0:'UN'}, inplace=True)
        cols=resolvepivotdf.columns.tolist()
        if cols[0]==0: #handle when there are unresolved conversations
            cols=cols[1:-1]+[cols[0]]+[cols[-1]]
            resolvepivotdf=resolvepivotdf[cols]
        
        ###naming dependent on code outside of function###[1,2,3,4,12,24,25]
        #resolvepivotdf.rename(columns={1: '0-1', 2: '1-2', 3:'2-3',4:'3-4',12:'4-12',24:'12-24',25:'>24',0:'UN','Total':'Grand Total'}, inplace=True)
        
        #Write to sheets
        # Convert the dataframe to an XlsxWriter Excel object.        
        sheetname=dfnametoprocess[idx]
            
        responserow=5
        responsepivotdf[0].to_excel(writer, sheet_name=sheetname,startrow=responserow)
                 
        worksheet = workbook.sheetnames[sheetname]
        worksheet.write_string(0, 0,'Weekly Email Support Summary')
        worksheet.write_string(3, 0,'Email Response')        
        
        #merge_range(first_row, first_col, last_row, last_col, data[, cell_format])
        worksheet.merge_range(responserow-1,1,responserow-1,7, 'No. of hours taken to Respond',merge_format)
        worksheet.write_string(responserow-1, 0,'Category')
        within4hours=float(within4hours_resp[0])/responsepivotdf[0]['Total'][-1]*100
        if within4hours:
            worksheet.write_string(responserow+len(responsepivotdf[0])+2, 4, "{:.2f}".format(within4hours)+'% responded within 4hrs')
        else:
            within4hours=0.00
            worksheet.write_string(responserow+len(responsepivotdf[0])+2, 4, "{:.2f}".format(within4hours)+'% responded within 4hrs')
            
        summaryrow=responserow+len(responsepivotdf[0])+3
        worksheet.write_string(summaryrow, 0,'Summary:')
        worksheet.write_string(summaryrow+1, 0,'1) Total of ' + str(responsepivotdf[0]['Total'][-1]) + ' ('+ str(uniqueconv_resp[0]) +' conversations) email support cases. (Prev week: ' + str(responsepivotdf[1]['Total'][-1]) + ' ('+ str(uniqueconv_resp[1]) +' conversations))')
        worksheet.write_string(summaryrow+2, 0,'2) Unresolved emails: ' + str(unresolvedthisweek)+' ('+str(uniquedunresolved)+' conversations)') 
                
        worksheet.write_string(summaryrow+6, 0,'Email Resolve')
        
        resolverow=summaryrow+7
        worksheet.write_string(resolverow-1, 0,'Category')
        worksheet.merge_range(resolverow-1,1,resolverow-1,8, 'No. of Hours taken to Resolve',merge_format)
        
        resolvepivotdf.to_excel(writer, sheet_name=sheetname,startrow=resolverow)
        tagpivotdf.to_excel(writer, sheet_name=sheetname,startrow=resolverow+len(resolvepivotdf)+20)
        format1=workbook.add_format({'font_color': 'white'})
        worksheet.conditional_format(responserow+1,1,responserow+len(responsepivotdf[0]),len(responsepivotdf[0].columns), {'type':     'cell',
                                        'criteria': '=',
                                        'value':    0,
                                        'format':   format1})
        worksheet.conditional_format(resolverow+1,1,resolverow+len(resolvepivotdf),len(resolvepivotdf.columns), {'type':     'cell',
                                        'criteria': '=',
                                        'value':    0,
                                        'format':   format1})
        
        #generate piechart
        response_pie=responsepivotdf[0].iloc[-1][:-2]        
        resolve_pie=resolvepivotdf.iloc[-1][:-1]                
        
        data=[  Pie(labels=responsecolumnlabels,#response_pie.keys(),
                   values=response_pie.tolist(),#response_pie.values(),
                   name='Response',
                   hoverinfo='label+value+name',
                   type='pie',
                   hole=0.4,
                   sort=False,
                   domain={'x': [0, .48], 'y': [tosplit*0.3, 1]},
                   marker={'colors': ['rgb(0, 255, 0)',#0-1
                                      'rgb(60, 225, 60)',#1-2
                                      'rgb(90, 200, 90)',#2-3
                                      'rgb(120, 175, 120)',#3-4
                                      'rgb(255, 175, 0)']}#4-5
                                             ),
                Pie(labels=resolvecolumnlabels,#resolve_pie.keys(),
                   values=resolve_pie.tolist(),#resolve_pie.values(),
                   name='Resolve',
                   hoverinfo='label+value+name',
                   type='pie',
                   hole=0.4,
                   sort=False,
                   domain={'x': [.52, 1], 'y': [tosplit*0.3, 1]},
                   marker={'colors': ['rgb(0, 255, 0)',#0-1
                              'rgb(60, 225, 60)',#1-2
                              'rgb(90, 200, 90)',#2-3
                              'rgb(120, 175, 120)',#3-4
                              'rgb(140,165,140)',#4-12
                              'rgb(170,170,170)',#12-24
                              'rgb(255,175,0)',#>24
                              'rgb(255,0,0)'#un
                                  ]}
                                             )        
                ]
        
        annotations=[
                        {
                            "font": {
                                "size": 20
                            },
                            "showarrow": False,
                            "text": "Response<br>"+"{:.2f}".format(within4hours)+"%<4h",
                            "x": 0.190,
                            "y": tosplit*0.3+(1-tosplit*0.3)/2,
                            "xref":'paper',
                            "yref":'paper'                                             
                        },
                        {
                            "font": {
                                "size": 20
                            },
                            "showarrow": False,
                            "text": "Resolve",
                            "x": 0.795,
                            "y": tosplit*0.3+(1-tosplit*0.3)/2,
                            "xref":'paper',
                            "yref":'paper'
                        }
                                ] 
        layout=dict(   title='Weekly Email Distribution - ' + dfnametoprocess[idx],
                           showlegend= False,
                           annotations=annotations                     
                            )
        
        prev4label=['Last '+plottf,'Two '+plottf+'s ago','Three '+plottf+'s ago','Four '+plottf+'s ago','Five '+plottf+'s ago']
        prev4label.reverse()
        
        
        
        if tosplit:
            total_resp=[sum(x) for x in zip(within4hours_resp, over4hours_resp)]
            percent_resp=[float(x)/y*100 for x,y in zip(within4hours_resp, total_resp)]
            
            #need to reverse the lists for output and skip first value
            within4hoursbar = Bar(x=prev4label, y=within4hours_resp[::-1], name='Within 4 hours', xaxis='x2', yaxis='y2')
            data.append(within4hoursbar)
            over4hoursbar = Bar(x=prev4label, y=over4hours_resp[::-1], name='Over 4 hours', xaxis='x2', yaxis='y2')
            data.append(over4hoursbar)
            '''
            avgresponse = Scatter(x=day_piv.columns, y=mean_piv/3600.0,
                                     name='Average Response time',yaxis='y2')    
            data_piv.append(avgresponse)
            
            longestresponse = Scatter(x=day_piv.columns, y=max_piv/3600.0,
                                         name='Longest Response time', yaxis='y2')    
            data_piv.append(longestresponse)
            '''    
            layout['yaxis2']=dict(title='Conversations',domain=[0, 0.25], anchor='x2')
            layout['xaxis2']=dict(title='Time',domain=[0.05, 0.95], anchor='y2')
            layout['barmode']='relative'
            
            annotationsbar=[dict(x=xi,y=yi,
                                 text="{:.2f}".format(zi)+"%<4hrs",
                                 xanchor='center',
                                 yanchor='bottom',                                 
                                 showarrow=False,
                                 ) for xi, yi, zi in zip(prev4label, total_resp[::-1],percent_resp[::-1])]
            
            layout['annotations']=annotations+annotationsbar
                            
                            #yaxis2=dict(title='Time(hours)',titlefont=dict(color='rgb(148, 103, 189)'),
                            #                  tickfont=dict(color='rgb(148, 103, 189)'),
                            #                  overlaying='y', side='right'
                            #              )
                            #annotations=[   dict(x=xi,y=yi, text=str(yi),
                            #                xanchor='center', yanchor='bottom',
                            #                showarrow=False) for xi, yi in zip(day_piv.columns, convocount.values)]
        fig=dict(data=data, layout=layout)
        plot(fig,filename=ofilename[:-5]+'_'+sheetname+'_pie.html',auto_open=False)        
    
    # Close the Pandas Excel writer and output the Excel file.
    #workbook.close()    
    writer.save()

#%% generate stats for each admin <need fix too many previous changes> 
'''
To do list
Weekly stats
Monthly stats

Stats:
Overall Response time (O)
Weekday Response time - during office hours (WDDOH)
Weekday Response time - after office hours (WDAOH)
Weekend Response time (WE)
Overall Resolve time <-- issue dependent
Weekday Resolve time - during office hours
Weekday Resolve time - after office hours
Weekend Resolve time
Total number of closed conversations (CC)
Total number of open conversations EOD (OC)
Issue list breakdown pie chart
Intercom participation rating


#Graphs
Lifetime Processed conversations - (created_at, first_response, last_closed) vs time (continuous)
Average Response/resolve time Bar chart - O,WDDOH,WDAOH,WE,CC,OC vs time (Weekly)
Average Response/resolve time Bar chart - O,WDDOH,WDAOH,WE,CC,OC vs time (Monthly)
'''

def genstatadmin(inputdf,ofilefolder,silent=True):
#keep copy for printing 
    groupedbyadminname = inputdf.groupby('adminname')    
    for i, item in groupedbyadminname:
        toprint=groupedbyadminname.get_group(i).sort_values('created_at',ascending=True)
        toprint['s_to_first_response']=toprint.s_to_last_closed.apply(lambda s: af.changenonetotimedeltazero(s))
        toprint['s_to_last_closed']=toprint.s_to_last_closed.apply(lambda s: af.changenonetotimedeltazero(s))
        toprint['s_to_first_closed']=toprint.s_to_first_closed.apply(lambda s: af.changenattotimedeltazero(s))
        toprint['s_to_last_update']=toprint.s_to_last_update.apply(lambda s: af.changenonetotimedeltazero(s))
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
        #plot(fig,filename=os.path.abspath(os.path.join(ofilefolder,i+'.html')))
        if not silent:
             plot(fig,filename=ofilefolder+i+'.html')
        else:
             plot(fig,filename=ofilefolder+i+'.html',auto_open=False)
