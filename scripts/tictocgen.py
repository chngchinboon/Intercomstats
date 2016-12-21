# -*- coding: utf-8 -*-
"""
Created on Wed Oct 05 09:16:37 2016
Code adapted from http://stackoverflow.com/a/26695514
@author: Benben
"""
import time

# Generator that returns time differences
def TicTocGenerator():    
    initaltime = 0           
    finaltime = time.time() 
    while True:
        initaltime = finaltime
        finaltime = time.time()
        yield finaltime-initaltime # Time difference

TicToc = TicTocGenerator() # create an instance of the TicTocGen generator

def toc(start=True):
    # Prints the time difference yielded by generator instance TicToc
    timeDiff = next(TicToc)
    if start:
        print( "Elapsed time: %f seconds.\n" %timeDiff )

def tic():
    # Records a time in TicToc, marks the beginning of a time interval
    toc(False)