# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 13:50:30 2017

@author: Owner
"""
from __future__ import print_function
import os
import numpy as np
np.random.seed(1337)
import matplotlib.pyplot as plt
import pandas as pd

from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.utils.np_utils import to_categorical
from keras.layers import Dense, Input, Flatten
from keras.layers import Conv1D, MaxPooling1D, Embedding
from keras.models import Model
import sys

#from sklearn.cross_validation import train_test_split

BASE_DIR = os.path.dirname( __file__ )
GLOVE_DIR = BASE_DIR + '/glove.6B/'
TEXT_DATA_DIR = BASE_DIR + '/20_newsgroup/'
MAX_SEQUENCE_LENGTH = 1000
MAX_NB_WORDS = 20000
EMBEDDING_DIM = 100
VALIDATION_SPLIT = 0.2

# first, build index mapping words in the embeddings set
# to their embedding vector

print('Indexing word vectors.')

embeddings_index = {}
f = open(os.path.join(GLOVE_DIR, 'glove.6B.100d.txt'))
for line in f:
    values = line.split()
    word = values[0]
    coefs = np.asarray(values[1:], dtype='float32')
    embeddings_index[word] = coefs
f.close()

print('Found %s word vectors.' % len(embeddings_index))

# second, prepare text samples and their labels
print('Processing text dataset')


#load from csv
outputfolder=os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, 'output'))
floc=os.path.abspath(os.path.join(outputfolder,'topconvstats.csv'))    
topconvdfcopy=pd.read_csv(floc, sep='\t', encoding='utf-8',index_col=False)
if hasattr(topconvdfcopy, u'Unnamed: 0'): del topconvdfcopy['Unnamed: 0']#might be hiding poorly merge attempts
if hasattr(topconvdfcopy, u'Unnamed: 0.1'): del topconvdfcopy['Unnamed: 0.1']#might be hiding poorly merge attempts
if hasattr(topconvdfcopy, 'convid'): topconvdfcopy['convid']=topconvdfcopy['convid'].astype('unicode')#loading auto changes this to int

print ('Loaded file from ' + floc)  

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

dataset=expandtag(topconvdfcopy,'issue').copy()
dataset=dataset.reset_index()
dataset=dataset[['issue','firstmessage']]
#remove those with no messages
dataset=dataset[~(dataset.firstmessage=='None')]
#remove those with no tags
dataset=dataset[~(dataset.issue=='None')]

#print info of dataset
dataset.groupby('issue').describe()                
dataset_length= dataset['issue'].map(lambda text: len(text))
#dataset_length.plot(bins=20, kind='hist')
dataset_length.describe()
dataset_distribution=dataset.groupby('issue').count().sort_values('firstmessage',ascending=False)



#data is too poorly conditioned and biased, use only top 6 and the rest put as Unknown <----- doesn't really improve results :(
issuetoclassify=['Login Help','Forward to School','Check In/Out','Admin','Portfolio','LFR','Unknown']
#issuetoclassify=['Login Help','Unknown']

def modissue(s,issuelist):
     if s not in issuelist:
          s='Unknown'
     return s

dataset['issue']=dataset.issue.apply(lambda s: modissue(s,issuetoclassify))
#dataset.groupby('label').count() 
issuename=issuetoclassify
issuename.sort()
#prep for keras
#prepare dictionary mapping for label name to numeric id

'''
issuename = []
with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir,'issuelist.txt'))) as inputfile:
    for line in inputfile:
        issuename.append(line.strip())
'''        


texts = []  # list of text samples
labels_index = pd.Series(sorted(issuename)).to_dict()  # dictionary mapping label name to numeric id
index_labels={v: k for k, v in labels_index.iteritems()}
labels = []  # list of label ids

texts=dataset['firstmessage'].tolist()
texts = [s.encode('ascii', 'ignore') for s in texts]
labels=dataset['issue'].tolist()
labels = [index_labels[s] for s in labels]

'''
for name in sorted(os.listdir(TEXT_DATA_DIR)):
    path = os.path.join(TEXT_DATA_DIR, name)
    if os.path.isdir(path):
        label_id = len(labels_index)
        labels_index[name] = label_id
        for fname in sorted(os.listdir(path)):
            if fname.isdigit():
                fpath = os.path.join(path, fname)
                if sys.version_info < (3,):
                    f = open(fpath)
                else:
                    f = open(fpath, encoding='latin-1')
                texts.append(f.read())
                f.close()
                labels.append(label_id)
'''
print('Found %s texts.' % len(texts))

# finally, vectorize the text samples into a 2D integer tensor
tokenizer = Tokenizer(nb_words=MAX_NB_WORDS)
tokenizer.fit_on_texts(texts)
sequences = tokenizer.texts_to_sequences(texts)

word_index = tokenizer.word_index
print('Found %s unique tokens.' % len(word_index))

data = pad_sequences(sequences, maxlen=MAX_SEQUENCE_LENGTH)

labels = to_categorical(np.asarray(labels))
print('Shape of data tensor:', data.shape)
print('Shape of label tensor:', labels.shape)

# split the data into a training set and a validation set
indices = np.arange(data.shape[0])
np.random.shuffle(indices)
data = data[indices]
labels = labels[indices]
nb_validation_samples = int(VALIDATION_SPLIT * data.shape[0])

x_train = data[:-nb_validation_samples]
y_train = labels[:-nb_validation_samples]
x_val = data[-nb_validation_samples:]
y_val = labels[-nb_validation_samples:]

print('Preparing embedding matrix.')

# prepare embedding matrix
nb_words = min(MAX_NB_WORDS, len(word_index))
embedding_matrix = np.zeros((nb_words + 1, EMBEDDING_DIM))
for word, i in word_index.items():
    if i > MAX_NB_WORDS:
        continue
    embedding_vector = embeddings_index.get(word)
    if embedding_vector is not None:
        # words not found in embedding index will be all-zeros.
        embedding_matrix[i] = embedding_vector

# load pre-trained word embeddings into an Embedding layer
# note that we set trainable = False so as to keep the embeddings fixed
embedding_layer = Embedding(nb_words + 1,
                            EMBEDDING_DIM,
                            weights=[embedding_matrix],
                            input_length=MAX_SEQUENCE_LENGTH,
                            trainable=False)

print('Training model.')

# train a 1D convnet with global maxpooling
sequence_input = Input(shape=(MAX_SEQUENCE_LENGTH,), dtype='int32')
embedded_sequences = embedding_layer(sequence_input)
x = Conv1D(256, 5, activation='relu')(embedded_sequences)
x = MaxPooling1D(5)(x)
x = Conv1D(256, 5, activation='relu')(x)
x = MaxPooling1D(5)(x)
x = Conv1D(256, 5, activation='relu')(x)
x = MaxPooling1D(35)(x)
x = Flatten()(x)
x = Dense(256, activation='relu')(x)
preds = Dense(len(labels_index), activation='softmax')(x)
'''
from keras.models import Sequential
from keras.layers import Dropout, Activation

nb_classes=len(issuename)+1
sequence_input = Input(shape=(MAX_SEQUENCE_LENGTH,), dtype='int32')
embedded_sequences = embedding_layer(sequence_input)

model = Sequential()
model.add(embedded_sequences)
model.add(Dense(512))
model.add(Activation('relu'))
model.add(Dropout(0.5))
model.add(Dense(nb_classes))
model.add(Activation('softmax'))
'''


model = Model(sequence_input, preds)
model.compile(loss='categorical_crossentropy',
              optimizer='rmsprop',
              metrics=['acc'])


# Train model
model.fit(x_train, y_train, validation_data=(x_val, y_val),
          nb_epoch=20, batch_size=128)

# Evaluate model
score, acc = model.evaluate(x_val, y_val, batch_size=128)

test=model.predict(x_val, batch_size=32, verbose=0)
    
print('Score: %1.4f' % score)
print('Accuracy: %1.4f' % acc)

def getmaxinnestedlist(s):
     outlist=[]
     for l in s:
          maxval=max(l)
          for idx,val in enumerate(l):
               if val==maxval:
                    outlist.append(idx)
     return outlist                    
          
testoutput_class=[issuename[i] for i in getmaxinnestedlist(test)]
testinput_class=[issuename[i] for i in getmaxinnestedlist(y_val)]
                 
testdf=pd.DataFrame([testoutput_class,testinput_class],index={'output','input'}).transpose()                 
testdf['result']=testdf.output==testdf.input
                                  





#word embedding layer     