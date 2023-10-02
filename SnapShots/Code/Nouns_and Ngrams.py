import re
import math
import statistics
# NLP stuff
import spacy
import nltk
from nltk.corpus import stopwords
from nltk.util import everygrams
from nltk import FreqDist
from nltk.tokenize import word_tokenize


def keyword_extractor(tweet_text, mode="word_list"):
    word_list = tweet_text.split()

    # Remove URLs
    cleaned_words = [word for word in word_list if not re.match(r'https?://\S+', word)]

    # Remove punctuation and convert to lowercase
    cleaned_words = [re.sub(r'[^\w\s]', '', word.lower()) for word in cleaned_words]

    # Remove stopwords
    stopwords_set = set(stopwords.words('english'))
    stopwords_set.update(["thats", "rt"])
    cleaned_words = [word for word in cleaned_words if word not in stopwords_set]

    if mode == "sentence":
        cleaned_words = " ".join(cleaned_words)

    return cleaned_words


def noun_phrase_extractor(tweet_text):
    # Load the spaCy English model
    # Download the model with python -m spacy download en_core_web_sm
    # or run spacy.cli.download("en_core_web_sm") into the code
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(tweet_text)
    # Extract noun phrases
    noun_phrases = [chunk.text for chunk in doc.noun_chunks]

    return noun_phrases


def ngram_noun_phrases(tweet_text_list):
    # Tokenize the texts and create bigrams
    tokenized_texts = [word_tokenize(text.lower()) for text in tweet_text_list]
    text_ngrams = [list(everygrams(tokens, min_len=2, max_len=5)) for tokens in tokenized_texts]

    # Flatten the list of ngrams
    all_ngrams = [ngram for sublist in text_ngrams for ngram in sublist]

    # Calculate the frequency of each bigram
    ngram_freq = FreqDist(all_ngrams)

    # Calculate the mean frequency of bigrams
    all_frequencies = list()
    for freq_value in ngram_freq.values():
        all_frequencies.append(freq_value)
    freq_std = statistics.pstdev(all_frequencies)
    freq_mean = statistics.mean(all_frequencies)
    target_frequency = math.ceil(freq_mean + freq_std)

    significant_ngrams = dict()

    for n_gram, n_freq in ngram_freq.items():
        if n_freq > target_frequency:
            n_gram = " ".join(n_gram)
            significant_ngrams[n_gram] = n_freq

    return significant_ngrams


if __name__ == "__main__":
    print("A")

