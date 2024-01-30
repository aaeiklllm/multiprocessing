"""
LAYUG Mikaella Louise D
TRANI Giancarlo Gabriel T

CMSC 135 LECTURE | Multiprocessing Challenge

Utilizing the multiprocessing module, this Python program determines
the following given the dataset movie_reviews_dataset.csv:
    1. Top 100 tokens based on frequency in the dataset
    2. Unique tokens/words in the dataset
Both outputs could be seen in two different text files.
(top_100_tokens.txt, unique_tokens.txt)
"""

import datetime
from multiprocessing import Pool

"""
preprocess_text (from timi_preprocessing.py)

parameters
    text (str): text to be processed

returns
    tokens (list): words/tokens from preprocessed text
"""
def preprocess_text(text: str):
    replace_with_space = ['\n', '/']
    for to_replace in replace_with_space:
        text = text.replace(to_replace, ' ')

    replace_with_nothing = ['"', "''", '.', '!', '?', '(', ')', '=', ';', ':', ',', "'s", '{', '}' '“', '“', '”', '‘', '’', '«', '»', '¨', '·', '£', '₤', '$', '#', '@', '§', '[', ']', "*", '~', '&', '^']
    for to_replace in replace_with_nothing:
        text = text.replace(to_replace, '')

    text = text.lower()
    text = text.split(" ")

    tokens = []
    for i in range(0, len(text)):
        stripped_token = text[i].strip().strip("'")
        if (stripped_token.isalpha()):
            tokens.append(stripped_token)
        
    return tokens

"""
write_to_file

parameters
    items (iterable): preprocessed word tokens
    filename (str): name of the fie where the data will be written
"""
def write_to_file(items, filename):
    with open(filename, 'w', encoding='utf-8') as file:
        file.write("\n".join(items))

"""
count_occurences

parameters
    tokens (str): preprocessed word tokens

returns
    word_count (dict): word token - key; occurence - value
"""
def count_occurrences(tokens):
    word_count = {}
    for token in tokens:
        if token in word_count:
            word_count[token] += 1
        else:
            word_count[token] = 1
    return word_count

"""
readFile

parameters
    filename (str): file name of the dataset

returns
    dataset (str): the content of the dataset
"""
def readFile(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        dataset = file.read()

    return dataset

"""
main

The main function of the program.
The Pool.map() function of the multiprocessing module is used to determine the top 100 tokens.
The set() function convertes the preprocessed list to obtain the unique tokens.
""" 
def main():
    start = datetime.datetime.now()
    
    # preprocess text obtained from the dataset
    preprocessed = preprocess_text(readFile("movie_reviews_dataset.csv")) 

    # Utilize pool from the multiprocesing module to count word occurences
    pool = Pool()
    word_count = pool.map(count_occurrences, [preprocessed])

    # combine dictionaries in word_count to a single dictionary
    combined_word_count = {}
    for i in word_count:
        for token, count in i.items():
            if token in combined_word_count:
                combined_word_count[token] += count
            else:
                combined_word_count[token] = count

    # sort in descending order and obtain the top 100 tokens
    sorted_word_counts = sorted(combined_word_count.items(), key = lambda x: x[1], reverse = True)
    top_100_tokens = [token for token, _ in sorted_word_counts[:100]]

    # use set to obtain unique tokens 
    unique_tokens = set(preprocessed)

    # write the unique tokens and top 100 token in different files
    write_to_file(unique_tokens, 'unique_tokens.txt')
    write_to_file(top_100_tokens, 'top_100_tokens.txt')

    end = datetime.datetime.now()
    print("Duration:" , end - start)

if __name__ == "__main__":
    main()