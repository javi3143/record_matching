import pandas as pd
from record_matching.Similarity_Metrics.sim import jaccard_similarity,jaro_similarity
from record_matching.ETL.etl import hide_people_names, clean_df, delete_noinfo

def matching(pairs,df_1,clean_df_1,df_2,clean_df_2,words_bag,column_name_left,column_name_right,delnum):
    
    # Hides data that contains people's names
    for col in df_1.columns:
        df_1[col] = df_1[col].apply(lambda x: hide_people_names(x))
    # Removes word from string if it is in words_bag
    clean_df_1[column_name_left] = clean_df(clean_df_1,column_name_left,delnum)[column_name_left].apply(lambda x: delete_noinfo(x,words_bag))
    clean_df_2[column_name_right] = clean_df(clean_df_2, column_name_left,delnum)[column_name_right].apply(lambda x: delete_noinfo(x,words_bag))
    # For every candidate pair, jaro and jaccard are applied and the best match is stored
    max_score = {}
    for x,y in pairs:
        try:
            score = jaccard_similarity(clean_df_1[column_name_left].loc[int(x)], \
                                        clean_df_2[column_name_right].iloc[int(y)])
            score_2 = jaro_similarity(clean_df_1[column_name_left].loc[int(x)], \
                                            clean_df_2[column_name_right].iloc[int(y)])
        except:
            continue
        try:
            max_score[x]
            if score >= max_score[x][1] and score_2 >= max_score[x][2]:
                max_score[x] = (y,score,score_2)
        except:
            max_score[x] = (y,score,score_2)

    if column_name_left == column_name_right:
        column_name_new = column_name_right + ' 2'
    # Creates dataframe to output results
    df_matches = pd.DataFrame()
    df_matches['ratepayerID'] = max_score.keys()
    df_matches[column_name_left] = df_matches['ratepayerID'].apply(lambda x: df_1[column_name_left].loc[int(x)])
    ids = [max_score[i][0] for i in max_score]
    df_matches['ID'] = ids
    try:
        column_name_new
        df_matches[column_name_new] = df_matches['ID'].apply(lambda x: df_2[column_name_right].iloc[int(x)])
    except:
        df_matches[column_name_right] = df_matches['ID'].apply(lambda x: df_2[column_name_right].iloc[int(x)])
    score_jaccard = [max_score[i][1] for i in max_score]
    score_jaro = [max_score[j][2] for j in max_score]
    df_matches['score_jaccard'] = score_jaccard
    df_matches['score_jaro'] = score_jaro

    result = []
    result.append(df_matches[df_matches[column_name_left] != 'private people data'].sort_values(['ratepayerID']))
    df_matches = df_matches[(df_matches['score_jaccard'] >= 0.93) & (df_matches['score_jaro'] >= 0.91)]
    
    return (result[0],df_matches[df_matches[column_name_left] != 'private people data'].sort_values(['ratepayerID']))