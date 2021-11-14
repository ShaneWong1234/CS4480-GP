import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import PorterStemmer
import pandas as pd
from string import punctuation


stopwordz = stopwords.words('english')
punctuationz = list(punctuation)

df = pd.read_csv("Combined_Dataset.csv")
headings = df.Heading.to_list()
stemHeadList = []

PS = PorterStemmer()

def sentenceStemmer(sentence):
    wordsTokens = word_tokenize(sentence)
    cleanseTokens = [wordsTokens for wordsTokens in wordsTokens if wordsTokens not in stopwordz and wordsTokens not in punctuationz]
    stemmedSentence = []
    print(cleanseTokens)
    for each in cleanseTokens:
        stemmedSentence.append(PS.stem(each))
        stemmedSentence.append(" ")
    return "".join(stemmedSentence)

for each in headings:
   stemHeadList.append(sentenceStemmer(each))

df2 = pd.DataFrame(stemHeadList)
df2.to_csv('ProcessedDataset.csv', index=False, header=False)