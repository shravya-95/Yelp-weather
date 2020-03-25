def modifyJson(fin, fout):
    replacedLine = "["
    prevOriginalLine = ""

    for line in fin:
        fout.write(replacedLine)  # Adds [ to the first line and then replaced line in subsequent iteratins
        prevOriginalLine = line
        replacedLine = line.replace('}\n', '},\n')

    fout.write(prevOriginalLine + "]")  # Appends ] to the last line
    fin.close()
    fout.close()


if __name__ == '__main__':
    fin = open("/home/ec2-user/yelpdata/business.json", "r")
    fout = open("/home/ec2-user/yelpdata/business_modified.json", "wt")
    modifyJson(fin, fout)
