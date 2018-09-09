def jaccard_similarity(query, document):
    intersection = set(query).intersection(set(document))
    union = set(query).union(set(document))
    return round(float(len(intersection))/float(len(union)),2)

def jaro_similarity(x1,x2):
    s1 = len(x1)
    s2 = len(x2)
    threshold = (min([s1,s2]) // 2) - 1
    shortest = min([s1,s2])
    m = 0
    t= 0 
    for i in range(shortest):
        if i == x2[i:].find(x1[i]) + i:
            m += 1
        elif x2[abs(i-int(threshold)):i+int(threshold)+1].find(x1[i]) >= 0:
            m += 1
            t += 1
        else:
            pass
    l = 0

    for x,y in zip(x1,x2):
        if l < 4:
            if x == y:
                l += 1
        else:
            break
    if m == 0:
        return 0
    else:
        jaro = (1/3) * (m/s1 + m/s2 + (m-(t/2))/m)
        return round(jaro + (l*0.1*(1-jaro)),2)