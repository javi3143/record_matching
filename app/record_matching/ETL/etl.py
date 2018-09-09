import pandas as pd
import numpy as np

# Removes symbols and lower case strings
def clean(x):
    whitelist = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o',\
                 'p','q','r','s','t','u','v','w','x','y','z','1','2','3','4',\
                 '5','6','7','8','9',' ']
    x = ''.join(c for c in x.lower() if c in whitelist)
    return " ".join(x.split())

# Removes words from a words_bag
def delete_noinfo(x,words_bag):
    name = x.split()
    [name.remove(i) for i in words_bag if i in name]
    name = ' '.join(w for w in name)
    return name

def clean_df(df,column_name_left,delnum):
    columns = df.columns
    # Removes numbers if blocking on address
    for col in columns:
        if delnum == 1:
            df[col] = df[col].apply(lambda x: ''.join([i for i in x if not i.isdigit()]))
    # Removes records whose name equals limited or ltd
    try:
        trash_df = df[(df[column_name_left] == 'limited') | (df[column_name_left] == 'ltd')]
        df = df.drop(trash_df.index.tolist())
        df = df.reset_index(drop=True)
    except:
        pass
    return df

# Replaces people's names to ensure anonymity
def hide_people_names(name):
    if (len(name.split(' ')[0]) == 1 and len(name.split(' ')) < 3) | (len(name.split(' ')[0]) == 1 and len(name.split(' ')[1]) == 1 and len(name.split(' ')) < 4):
        return 'private people data'
    else:
        return name

from record_matching.Blocking.blocking import blocking 

def csv_to_df(dataset_left,column_name_left,dataset_right,column_name_right,delnum):
    # Opens both datasets and cleans names
    with open(dataset_right+'.csv',encoding='utf-8') as data:
        companies_house = pd.read_csv(data,usecols=[column_name_right])
    clean_companies_house = pd.DataFrame()
    clean_companies_house = clean_companies_house.append(companies_house)
    for col in clean_companies_house.columns:
        clean_companies_house[col] = clean_companies_house[col].astype('str')
        clean_companies_house[col] = clean_companies_house[col].apply(lambda x: clean(x))
        
    with open('./static/'+dataset_left,encoding='windows-1252') as data:
        non_domestic_rates = pd.read_csv(data,usecols=[column_name_left])
    clean_non_domestic_rates = pd.DataFrame()
    clean_non_domestic_rates = clean_non_domestic_rates.append(non_domestic_rates)
    for col in clean_non_domestic_rates.columns:
        clean_non_domestic_rates[col] = clean_non_domestic_rates[col].astype('str')
        clean_non_domestic_rates[col] = clean_non_domestic_rates[col].apply(lambda x: clean(x))
    
    # Generates words_bag containing 20 most frequent words
    norm_companies_house_words = pd.read_csv('./static/watson_words.csv',index_col='Unnamed: 0',nrows=20)
    words_non_domestic_rates = pd.DataFrame()
    words_non_domestic_rates = words_non_domestic_rates.append(clean_non_domestic_rates)
    non_domestic_rates_list = words_non_domestic_rates[column_name_left].tolist()
    non_domestic_rates_names = [i.split() for i in non_domestic_rates_list]
    flat_non_domestic_rates_names = np.concatenate(non_domestic_rates_names)
    names_ra = pd.Series(flat_non_domestic_rates_names).value_counts().keys().tolist()
    non_domestic_rates_df = pd.DataFrame({
    'counts': pd.Series(flat_non_domestic_rates_names).value_counts().tolist(),
    }, index=names_ra)

    norm_non_domestic_rates_words=(non_domestic_rates_df-non_domestic_rates_df.min())/ \
                        (non_domestic_rates_df.max()-non_domestic_rates_df.min())
    mixed_bag = pd.concat([norm_companies_house_words,norm_non_domestic_rates_words])
    mixed_bag = mixed_bag.groupby(mixed_bag.index.str.lower()).mean()

    mixed_bag = mixed_bag.sort_values('counts', ascending=False)
    mixed_bag_list = pd.Series(mixed_bag.index).tolist()[0:20]
    
    return blocking(non_domestic_rates,clean_non_domestic_rates, companies_house,clean_companies_house,mixed_bag_list,column_name_left,column_name_right,delnum)