def get_codes(file):
    codes={}
    with open(file) as fp:
        for line in fp:
            line=line.split(",")
            codes[line[0]]=True
    return codes
