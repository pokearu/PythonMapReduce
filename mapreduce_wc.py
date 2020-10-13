import marshal

def word_count(file_name:str, file_contents: str):
    return [(word,1) for word in file_contents.split()]

def reduce_words(key: str, words: list):
    count = 0
    for word in words:
        # if key == word[0]:
        count = count + int(word[1],10)
    return (key, count)

with open('map_wc', 'wb') as f:
    f.write(bytearray(marshal.dumps(word_count.__code__)))
with open('reduce_wc', 'wb') as f:
    f.write(bytearray(marshal.dumps(reduce_words.__code__)))