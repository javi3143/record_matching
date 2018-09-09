import pandas as pd
from record_matching.ETL.etl import clean_df
from record_matching.Matching.matching import matching
def blocking(df_1,clean_df_1,df_2,clean_df_2,words_bag,column_name_left,column_name_right,delnum):
    # Prepares data prior concatenation
    df_left = pd.DataFrame()
    df_right = pd.DataFrame()
    df_left = df_left.append(clean_df_1)
    df_right = df_right.append(clean_df_2)
    df_left.columns = ['name']
    df_right.columns = ['name']
    df_left['dataset'] = 0
    df_right['dataset'] = 1
    # Concatenates both datasets
    df_concat = pd.concat([df_left,df_right])
    # Shuffles data 
    df_concat = df_concat.sample(frac=1)
    # Sorts values alphabetically
    df_concat = df_concat.sort_values('name')
    # Adds a new index
    df_concat = df_concat.reset_index()
    # Selects window size
    w = 20
    # Generates record matching candidate pairs
    pairs = []
    for index in df_concat.index:
        if df_concat['dataset'].iloc[index] == 0:
            for i in df_concat.index[index - w:index+w+1]:
                if df_concat['dataset'].iloc[i] == 1:
                    pairs.append((df_concat['index'].iloc[index],df_concat['index'].iloc[i]))
                    control = index
                    while df_concat['name'].iloc[control-1] == df_concat['name'].iloc[index] and control > 0:
                        pairs.append((df_concat['index'].iloc[control-1], df_concat['index'].iloc[i]))
                        control -= 1
                    control = index
                    while df_concat['name'].iloc[control+1] == df_concat['name'].iloc[index] and control < len(df_concat)-2:
                        pairs.append((df_concat['index'].iloc[control-1], df_concat['index'].iloc[i]))
                        control += 1
    return matching(pairs,df_1,clean_df_1,df_2,clean_df_2,words_bag,column_name_left,column_name_right,delnum)