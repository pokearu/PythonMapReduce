import marshal

def word_count(file_name:str, file_contents: str):
    lines = file_contents.split('\n')
    res = []
    for line in lines:
        # Remove spaces from beginning and end of the line
        line = line.strip()
        # Split it into words
        words = line.split()
        # Output tuples on stdout
        res = res + [(word,1) for word in words]
        # for word in words:
        #     print('{0}\t{1}'.format(word, "1"))
    return res
print(marshal.dumps(word_count.__code__))