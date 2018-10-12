import re, json, math, sys, time
from time import time
from collections import defaultdict, namedtuple

from urllib.parse import urlparse
from pymongo import MongoClient

from bs4 import BeautifulSoup
from nltk.stem.snowball import SnowballStemmer

## before removing stopwords and .txt file forat, from database: 340k unique TERMS
## after : 140489 terms
stopwords = [
"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at",
"be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did",
"do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have",
"having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself",
"his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "it", "it's", "its",
"itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other",
"ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll", "she's",
"should", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
"then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those",
"through", "to", "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've",
"were", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
"why", "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
"yourselves" ]

# helper
def isGoodLink(url):
    parsed = urlparse(url)
    if parsed.path == 'ironwood.ics.uci.edu':
        return False
    flformat = parsed.path.split('.')[-1]
    # if flformat not in ['html', 'htm', 'php', 'txt']:
    if flformat not in ['html', 'htm', 'php']:
        return False
    return True


# helper2
def redirectedUrl(url):
    if '~mlearn/datasets' in url:
        url = 'http://archive.ics.uci.edu/ml/index.php'
    if 'BadContent?' in url:
        url = 'https://www.ics.uci.edu/~dechter/software.html'
    return url


print( '    ****    ****    START    ****    ****    \n' )
staTime = time()
## The tf-idf won't be computed before put terms into the database
## the database contains {term: postings} pairs and for each term's postings,
## it contains the {docid: counts} pairs for the term appeared in each docid


## create a mongodb database => wordsdb
client = MongoClient('mongodb://localhost:27017')
termdb = client.wordsdb
invindex = termdb.invindex
## sbs is the word stemmer
sbs = SnowballStemmer("english")

jsindex = open('WEBPAGES_RAW/bookkeeping.json', 'r').read()
bkkplst = json.loads(jsindex)


# wordDict = defaultdict(dict)
wordDict = defaultdict( lambda: defaultdict( lambda: [int, str] ) )

# wordDict = defaultdict( lambda: defaultdict( lambda: [int, str] ) )
# wordDict = defaultdict( lambda: defaultdict( lambda: namedtuple(Posti, ['ct', 'url']]) ) )

## iterate through the bkkeeping list each docid(00/00)
## {docid : webUrl}
def getDict():
    pass


for docid in bkkplst:
    # webUrl  = bkkplst[docid]
    # fileDir = WEBPAGES_RAW/docid
    fileDir = 'WEBPAGES_RAW/{}'.format(docid)
    webUrl = redirectedUrl(bkkplst[docid])

    if isGoodLink(webUrl):
        sys.stdout.write('.')
        sys.stdout.flush()

        with open(fileDir, 'r') as webFile:
            parsedFile = BeautifulSoup(webFile, "html.parser")

            # remove javascript and style words
            for script in parsedFile(['script', 'style', 'iframe', 'a']):
                script.decompose()
            # them form the words lists
            textlst = re.findall(r"\w+", parsedFile.get_text())
            ## stem each word and then store in wordDict
            ## wordDict{'word1': {'did1':?, 'did2':?, 'did10':?},'word2': {'did8':?, 'did25':?, 'did109':?},}
            for term in textlst:
                # modified here 5/27
                if term not in stopwords:
                    term = sbs.stem(term).lower()
                    # wordDict{term: posting}       # posting{docid: [ct,url]}
                    # wordDict{ term: posting{docid: [ct,url]} }
                    if type(wordDict[term][docid][0]) == type:
                        wordDict[term][docid][0] = 0
                    wordDict[term][docid][0] += 1 # the ?
                    wordDict[term][docid][1] = webUrl

                # wordDict['t1']['p2'] = (10, 'url2')

midTime = time()
s = round(midTime - staTime)
m = math.floor(s / 60)
s = s - 60 * m
print( '\n\n\n  .................   Cost {} min {} seconds \n\t\t to generate the wordDict\n'.format(m, s))

# Total document size, which is the N
docsz = len(wordDict)
# Hold certain amount of dicts
# buffer = list()
buffer = [{'_totalSize_': docsz}]


# print some info for testing perpose
cc = 0
for tm, post in wordDict.items():
    cc +=1
    print(tm, len(post), '\n\n')
    for dcid, lst in post.items():
        print('\t\t', lst[0], ' : ', lst[1][:70])
    if cc >5:
        break;

print('\n\t~\t~\tunique size : {}'.format(docsz))
print('\n\t~\t~\tdone printing\t~\t~\t\n')
print('\n    ....    ....    NOW DUMPING DATA INTO DATABASE    ....    ....    \n' )


# Calculate tf-idf through the wordDict
# then store the updated dict into database
for term, posting in wordDict.items():
    ## w(tf-idf) == w(tf) * w(idf)
    ## w(tf) = 1+ log(tf)
    ## w(idf) = log(N/df)
    idf = math.log10( docsz / len(posting) )
    ## update raw tf count to tf-idf
    for docid, cturl in posting.items():
        cturl[0] = (1 + math.log10(cturl[0])) * idf

    ## store {term:posting, ...} into buffer list
    ## posting{docid:[ct, url], ...}
    ## invindex['posting'] = [docid][0] / [1]
    buffer.append({'term':term, 'posting':posting})

    ## dump buffer into database
    if len(buffer) == 1000:
        invindex.insert_many(buffer)
        buffer = list()

if len(buffer) > 0:
    invindex.insert_many(buffer)
    buffer = list()





endTime = time()
s = endTime - midTime


print('    ****    ****    Used {} seconds to dump data to database\n'.format(s))

print( '    ****    ****    DONE    ****    ****    \n' )
##
