# -*- coding: iso-8859-15 -*-
import string
#========= Normalise the string
def normalize(s):
    for p in string.punctuation:
        s = s.replace(p, '')

    return s.lower().strip()

def posCounter(sentence, listPos):
    p_counter = 0
    for word in sentence:
        if word in listPos:
            p_counter+=2
    return p_counter
def negCounter(sentence, listNeg):
    n_counter = 0
    for word in sentence:
        if word in listNeg:
            n_counter-=2
    return n_counter
def findIncrementor(sentence, listIncr, listPos, listNeg):
    incr_counter = 0
    incr_list = set(sentence).intersection(listIncr)
    for incr in incr_list:
        indx_incr = indx_incr_original = sentence.index(incr)
        while indx_incr>0:
            closer_pos_index_r = closer_neg_index_r = closer_pos_index_f = closer_neg_index_f =99
            if sentence[indx_incr] in listPos:
                closer_pos_index_r = indx_incr_original-indx_incr
                break
            if sentence[indx_incr] in listNeg:
                closer_neg_index_r = indx_incr_original-indx_incr
                break
            indx_incr-=1
        while indx_incr<len(sentence):
            if sentence[indx_incr] in listPos:
                closer_pos_index_f=indx_incr-indx_incr_original
                break
            if sentence[indx_incr] in listNeg:
                closer_neg_index_f = indx_incr-indx_incr_original
                break
            indx_incr+=1
        list_indexes = [closer_pos_index_r, closer_neg_index_r, closer_pos_index_f, closer_neg_index_f]
        min_value_index = min(list_indexes)
        if min_value_index==0 or min_value_index==2:
            incr_counter+=1
        if min_value_index==1 or min_value_index==3:
            incr_counter-=1
    return incr_counter
def findDecrementor(sentence, listDecr, listPos, listNeg):
    decr_counter = 0
    decr_list = set(sentence).intersection(listDecr)
    for decr in decr_list:
        indx_decr = indx_decr_original = sentence.index(decr)
        while indx_decr>0:
            closer_pos_index_r = closer_neg_index_r = closer_pos_index_f = closer_neg_index_f =99
            if sentence[indx_decr] in listPos:
                closer_pos_index_r = indx_decr_original-indx_decr
                break
            if sentence[indx_decr] in listNeg:
                closer_neg_index_r = indx_decr_original-indx_decr
                break
            indx_decr-=1
        while indx_decr<len(sentence):
            if sentence[indx_decr] in listPos:
                closer_pos_index_f=indx_decr-indx_decr_original
                break
            if sentence[indx_decr] in listNeg:
                closer_neg_index_f = indx_decr-indx_decr_original
                break
            indx_decr+=1
        list_indexes = [closer_pos_index_r, closer_neg_index_r, closer_pos_index_f, closer_neg_index_f]
        min_value_index = min(list_indexes)
        if min_value_index==0 or min_value_index==2:
            decr_counter-=1
        if min_value_index==1 or min_value_index==3:
            decr_counter+=1
    return decr_counter
def findInverter(sentence, listInv, listPos, listNeg):
    inv_counter = 0
    inv_list = set(sentence).intersection(listInv)
    for inv in inv_list:
        indx_inv = indx_inv_original = sentence.index(inv)
        while indx_inv>0:
            closer_pos_index_r = closer_neg_index_r = closer_pos_index_f = closer_neg_index_f =99
            if sentence[indx_inv] in listPos:
                closer_pos_index_r = indx_inv_original-indx_inv
                break
            if sentence[indx_inv] in listNeg:
                closer_neg_index_r = indx_inv_original-indx_inv
                break
            indx_inv-=1
        while indx_inv<len(sentence):
            if sentence[indx_inv] in listPos:
                closer_pos_index_f=indx_inv-indx_inv_original
                break
            if sentence[indx_inv] in listNeg:
                closer_neg_index_f = indx_inv-indx_inv_original
                break
            indx_inv+=1
        list_indexes = [closer_pos_index_r, closer_neg_index_r, closer_pos_index_f, closer_neg_index_f]
        min_value_index = min(list_indexes)
        if min_value_index==0 or min_value_index==2:
            inv_counter-=4
        if min_value_index==1 or min_value_index==3:
            inv_counter+=4
    return inv_counter
def spanishSentimentDetector(sentence):
    import os
    sentiment = ""
    os.chdir("/home/ubuntu/sociabyte/appTornado/dictionary")
    with open ("positive.txt", "r") as myfile:
        pos=myfile.read().replace('\n', ' ')
    pos_list = pos.split(" ")
    with open ("negative.txt", "r") as myfile:
        neg=myfile.read().replace('\n', ' ')
    neg_list = neg.split(" ")
    with open ("incrementer.txt", "r") as myfile:
        incrementer=myfile.read().replace('\n', ' ')
    incrementer_list = incrementer.split(" ")
    with open ("decrementer.txt", "r") as myfile:
        decrementer=myfile.read().replace('\n', ' ')
    decrementer_list = decrementer.split(" ")
    with open ("inverter.txt", "r") as myfile:
        inverter=myfile.read().replace('\n', ' ')
    inverter_list = inverter.split(" ")
    words = sentence.split(" ")
    pos_val = posCounter(words, pos_list)
    neg_val = negCounter(words, neg_list)
    incr_val = findIncrementor(words, incrementer_list, pos_list, neg_list)
    decr_val = findDecrementor(words, decrementer_list, pos_list, neg_list)
    inv_val = findInverter(words, inverter_list, pos_list, neg_list)
    sentiment_score = (pos_val+neg_val+incr_val+decr_val+inv_val)
    print sentiment_score
    print 'pos_val:', pos, "neg_val:", neg_val, "incr_val:", incr_val, "decr_val:", decr_val, "inv_val:", inv_val
    if sentiment_score<1 or sentiment_score>-1:
        sentiment = "neutral"
    if sentiment_score>0.5:
        sentiment = "positive"
    if sentiment_score<-0.5:
        sentiment = "negative"
    return sentiment
from langdetect import detect


sent = spanishSentimentDetector("Qué es esto?")
print sent, "===="







