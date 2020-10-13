import marshal

def word_match(file_name:str, file_contents: str):
    return [(word,file_name) for word in file_contents.split()]

def reduce_words(key: str, files: list):
    count = []
    for f in files:
        # if key == word[0]:
        count.append(f[1])
    return (key, list(set(count)))

with open('map_ii', 'wb') as f:
    f.write(bytearray(marshal.dumps(word_match.__code__)))
with open('reduce_ii', 'wb') as f:
    f.write(bytearray(marshal.dumps(reduce_words.__code__)))