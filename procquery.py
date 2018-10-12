import json, math, os, re
import queue

from collections import defaultdict
from pymongo import MongoClient

from bs4 import BeautifulSoup
from nltk.stem.snowball import SnowballStemmer

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

# jsindex = open('WEBPAGES_RAW/bookkeeping.json', 'r').read()
# bkkplst = json.loads(jsindex)

## the term database
termdb = MongoClient().wordsdb
invindex = termdb.invindex
## the word stemmer
sbs = SnowballStemmer("english")

## a generator to find all occurrence of the given substr
def findAllStr(a_str, sub):
    a_str = a_str.lower()
    sub = sub.lower()

    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += len(sub) # use start += 1 to find overlapping matches

## remove extra punctuations in the query string
def removePunc(astr):
    punc = [',', '.', '/', '?', '<', '>', ';', ':', '"', '"', "'", "'", '(', ')', '[', ']', '|',
            '!', '@', '#', '$', '%', '^', '&', '*', '-', '_', '+', '=', '{', '}', '`', '~']
    for p in punc:
        astr = astr.replace(p, '')
    return astr


# function to handle the raw query
def searchQuery(query):
    fileDir = os.path.join(os.getcwd(), 'WEBPAGES_RAW/bookkeeping.json')

    jsindex = open(fileDir, 'r').read()
    bkkplst = json.loads(jsindex)

    # remove the duplicates in the candidate query
    # add if q not in stopwords constrain to avoid a key error bug around line 185
    rawquery = list( {sbs.stem(q) for q in removePunc(query).split() if q not in stopwords} )
    # rawquery = list( {sbs.stem(q) for q in removePunc(query).split() } )

    print( '\n\n\n~~~~~~~~ ~~~~~~~~~ ~~~~~~~ rawquery : {} ~~~~~~~~ ~~~~~~~~~ ~~~~~~~\n\n\n'.format(rawquery))

    # result contains [(docid, url), ...] pairs
    result = []
    # if there is only one term in query, get the results url by sorting on tfidf of this term's posting list
    if len(rawquery) == 1:
        # term / posting  pair
        tpp = termdb.invindex.find_one({"term": rawquery[0]})
        # print('... ... len of tpp: {}\n'.format(len(tpp)))

        if tpp is not None:
            # tpp['posting'] = {docid: [ct, url]}       result list contain [(docid, url), ...] pairs
            srtedPosting = sorted(tpp['posting'].items(), key=lambda x:x[1][0], reverse=True)
            # [(docid, url), ...]   # discard any urls that is longer than 70 chars
            result = [ (x, y[1]) for (x,y) in srtedPosting if len(y[1])<70]
            # sort the posting list in decrease order of tf-idf
            # then extract (docid, url) pair of the top 10 (or all if less than 10)
            result = result if len(result)<10 else result[0:10]
            # print('\n ... result : {} ... \n'.format( result ))
        else:
            result = [('none', 'none')]
    # if there are more than one query term, call ConsineScore function, which return list of docids
    else:
        # print(' ...  running cosine score  ... \n')
        result = CosineScore(rawquery)


    # searchResult{url:snippets, ... }
    searchResult = defaultdict(lambda: set())
    # result[(docid, url), ...]
    if result[0][0] != 'none':
        for (docid, url) in result:
            fileDir = os.path.join(os.getcwd() + '/WEBPAGES_RAW/'+ docid)
            with open(fileDir, 'r') as html:
                soup = BeautifulSoup(html, "html.parser")

                # remove javascript and style words
                for script in soup(['script', 'style', 'iframe', 'a']):
                    script.decompose()

                text = soup.get_text()
                for q in rawquery:
                    # poslst[index1, index2, ...] contains all the index of q's occurrence in string text
                    poslst = list(findAllStr(text,q))
                    for pos in poslst:
                        pos1 = pos-40 if pos>40 else 0
                        pos2 = pos+60 if pos+60 < len(text) else len(text)
                        # delete extra newline char
                        snippet = re.sub('\n+', '\n', text[pos1:pos2])
                        # remove truncated chars, snip starts and ends with whitespace  # 'rd word  ... wo' --> ' word ... word '
                        snippet = snippet[snippet.find(' '):snippet.rfind(' ')]

                        if len(searchResult[url])<3:
                            searchResult[url].add(snippet)
    else:
        # result == [('none', 'none')] - no result
        searchResult[''].add('Your search - {} - did not match any documents'.format(query))

    # print(searchResult)
    # for url, snippets in searchResult.items():
    #     print('url: {}'.format(url))
    #     for n, ln in enumerate(snippets):
    #         print(n, '\n', ln, '\n')
    #     print('\n')

    # searchResult{url:snippet, ... }
    return searchResult




## function to calculate the cosine scores of words & queries   ## rawquery == terms
def CosineScore(rawquery):
    # retrieve total size from db
    N = termdb.invindex.find_one({"_totalSize_": {"$exists": True}})['_totalSize_']

    # a dict contain {queryterm: q_postings, ...} pairs
    queryPosting = {}
    # all the docids for this query
    allDocId = set()             ## {docids, ...}

    for term in rawquery:
        # find single term from database
        tpp = termdb.invindex.find_one({"term": term})      ## tpp['posting'] = posting{docid: [ct,url]}
        # if found, add the docids of this query term into allDocId
        if tpp is not None:
            # tpp['posting'] = {docid: [ct, url], ...}
            # queryPosting={qterm: qposting, ...} == {term: {docid1: [ct, url], docid2: [ct, url]}, ... }
            queryPosting[term] = tpp['posting']
            ## allDocId contains the all the docid of the 'term'
            allDocId.update(tpp['posting'].keys())  ## tpp['posting'].keys() == docid
            # allDocId.intersection_update(tpp['posting'].keys()) ## tpp['posting'].keys() == docid
        # if not, add nothing


    # EXTRACT DOCIDS THAT CONTAINS AT LEAST 65% OF THE QUERY TERMS (lower the rate from 75% down to 65%)

    # queryLst[(term, posting{}), ...]                 posting(docid, [tfidf, url])
    # sorted by increasing len(posting) aka (decreasing in idf)
    queryLst = sorted(queryPosting.items(), key=lambda x: len(x[1]))

    print(' ````````   ``` len queryLst : {}'.format(len(queryLst)))
    print(queryLst[0][0])


    # for almost 65% of query terms:
    numOfQueryTerms = math.ceil(len(queryLst) * 0.65)
    for i in range(0, numOfQueryTerms):
        ## allDocId now contains the docids (that contains 65% of more valueable query terms  )
        ## for 3-word queries, 3*0.75 = 3
        #  queryLst[i][1] = posting
        allDocId.intersection_update(queryLst[i][1].keys())

    # generate the dict of docs,
    # roughDocScore{doc1: {term1: tfidf1, term2: tfidf2...}, ...} format
    roughDocScore = defaultdict(lambda: defaultdict(float))
    for docid in list(allDocId):
        # queryPosting = {qterm: qposting, ...}
        # for term in queryPosting.keys():
        for (term, x) in queryLst:
            roughDocScore[docid][term] = queryPosting[term][docid][0] if docid in queryPosting[term].keys() else 0


    # final document score -- normalized score for each selected docu per query,
    # # finalDocScore{docid: [score, url], ...}
    finalDocScore = defaultdict(lambda:[lambda:double,lambda:str])
    for docid in roughDocScore.keys():
        docLen = 0
        # [tfidf1, tfidf2, ...]
        for weight in roughDocScore[docid].values():
            docLen += weight * weight
        docLen = math.sqrt(docLen)

        totalScore = 0
        for term in roughDocScore[docid]:
            totalScore += roughDocScore[docid][term]

        if tpp is not None:
            # print('\n\n\n is docid in tpp_pos  ?   :  ')
            # print(docid)
            # print(len(tpp['posting']), list(tpp['posting'].keys())[0:5])
            # print(docid in tpp['posting'])

            finalDocScore[docid] = [ totalScore / docLen, tpp['posting'][docid][1] ]

            # try:
            #     print('\n tpp_po_doc : {}\n'.format(tpp['posting'][docid]))
            #     finalDocScore[docid] = [ totalScore / docLen, tpp['posting'][docid][1] ]
            # except:
            #     print('\n tpp_po_doc  no key \n')
            #     finalDocScore[docid] = [0, '']

    # sort on totalScore, decreasing order.
    # srtedfinalDocScore[(docid, [scroe, url]), ...]
    srtedfinalDocScore = sorted(finalDocScore.items(), key=lambda x:x[1][0], reverse=True)
    # extract [(docid, url), ...] pairs
    result = [ (x, y[1]) for (x,y) in srtedfinalDocScore]
    # if no result found, return (none, none) to indicate   # for queries that cannot be found in database
    if len(result) == 0:
        result = [('none', 'none')]

    return result if len(result) <10 else result[0:10]




if __name__ =='__main__':
    # searchQuery("I'm")
    # searchQuery("I'm feeling dizzy.")
    # searchQuery('single feel alumni')
    searchQuery('increasingly important')
