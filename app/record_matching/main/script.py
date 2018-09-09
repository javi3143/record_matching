from record_matching.ETL.etl import csv_to_df
import pandas as pd

def urbantide_record_matching(dataset_left,column_name_left,dataset_right,column_name_right,delnum):
    return csv_to_df(dataset_left,column_name_left,dataset_right,column_name_right,delnum)


def evaluator(gtruth_file,col_right):
    results = pd.read_csv('./static/results.csv')
    try:
        gtruth = pd.read_csv('./static/'+gtruth_file)
    except:
        gtruth = pd.read_csv('./static/'+gtruth_file,encoding="ISO-8859-1")
    tp = pd.DataFrame().append(results)
    fp = pd.DataFrame().append(results)

    tp['ratepayer'] = results['ratepayer'].apply(lambda x: x if x in gtruth[col_right].tolist() else '')
    fp['ratepayer'] = results['ratepayer'].apply(lambda x: x if x not in gtruth[col_right].tolist() else '')
    tp[tp['ratepayer'] != ''].to_csv('./static/true_positives.csv',index=False)
    fp[fp['ratepayer'] != ''].to_csv('./static/false_positives.csv',index=False)
    counts = str(len(tp[tp['ratepayer'] != '']))+','+str(len(fp[fp['ratepayer'] != '']))
    return counts