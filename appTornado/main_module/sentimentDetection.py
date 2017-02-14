import collections, itertools
import nltk.classify.util, nltk.metrics
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import movie_reviews, stopwords
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from nltk.probability import FreqDist, ConditionalFreqDist
import time
from featx import bag_of_words

def best_word_feats_sent(words):
    return dict([(word, True) for word in words if word in bestwords])

def best_bigram_word_feats_sent(words, score_fn=BigramAssocMeasures.chi_sq, n=200):
    bigram_finder = BigramCollocationFinder.from_words(words)
    bigrams = bigram_finder.nbest(score_fn, n)
    d = dict([(bigram, True) for bigram in bigrams])
    d.update(best_word_feats_sent(words))
    return d

def evaluate_classifier_sent(featx):
    negids = movie_reviews.fileids('neg')
    posids = movie_reviews.fileids('pos')

    negfeats = [(featx(movie_reviews.words(fileids=[f])), 'neg') for f in negids]
    posfeats = [(featx(movie_reviews.words(fileids=[f])), 'pos') for f in posids]

    negcutoff = len(negfeats)
    poscutoff = len(posfeats)

    trainfeats = negfeats[:negcutoff] + posfeats[:poscutoff]
    #print trainfeats
    #testfeats = negfeats[negcutoff:] + posfeats[poscutoff:]
    #print testfeats
    testfeatsfile = open("testfeatsfile.txt", "w")
    print_neg = negfeats[:5]
    testfeatsfile.write(str(print_neg))
    testfeatsfile.close()
    classifier = NaiveBayesClassifier.train(trainfeats)
    return classifier

def findSentiment():
    word_fd = FreqDist()
    label_word_fd = ConditionalFreqDist()

    for word in movie_reviews.words(categories=['pos']):
        word_fd.inc(word.lower())
        label_word_fd['pos'].inc(word.lower())

    for word in movie_reviews.words(categories=['neg']):
        word_fd.inc(word.lower())
        label_word_fd['neg'].inc(word.lower())

    pos_word_count = label_word_fd['pos'].N()
    neg_word_count = label_word_fd['neg'].N()
    total_word_count = pos_word_count + neg_word_count

    word_scores = {}

    for word, freq in word_fd.iteritems():
        pos_score = BigramAssocMeasures.chi_sq(label_word_fd['pos'][word],
            (freq, pos_word_count), total_word_count)
        neg_score = BigramAssocMeasures.chi_sq(label_word_fd['neg'][word],
            (freq, neg_word_count), total_word_count)
        word_scores[word] = pos_score + neg_score

    best = sorted(word_scores.iteritems(), key=lambda (w,s): s, reverse=True)[:10000]
    global bestwords
    bestwords= set([w for w, s in best])

    sent_classifier_bi_sent=evaluate_classifier_sent(best_bigram_word_feats_sent)
    return sent_classifier_bi_sent