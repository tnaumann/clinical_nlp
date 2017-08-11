# -*- coding: utf-8 -*-
"""
# Created on 8/9/2017

@author: Dustin Doss (ddoss@mit.edu)

Build pandas dataframe from MIMIC 1.4 notes of different types.
"""

from time import time
import logging
import psycopg2
import cPickle as pickle
import os
import pandas as pd

output_dir = './data'
logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
CATEGORIES =  ["'physician'", "'nursing'", "'nursing/other'",
                  "'respiratory'", "'ecg'", "'radiology'", "'rehab services'",
                  "'nutrition'", "'pharmacy'", "'social work'",
                  "'case management'", "'general'", "'echo'", "'consult'"]

def build_notes_dataframe(time_period, genres=CATEGORIES):
    """
    Gets notes from the given set of genres

    """
    suffix = time_period
    if time_period == 'admit_to_24h_disch':
        time_query = """
                     and (n.chartdate >= f.admittime OR n.charttime >= f.admittime)
		     and (n.chartdate <= (f.dischtime - interval '1 day') OR
                     n.charttime <= (f.dischtime  - interval '1 day'))
                     """
    elif time_period == 'admit_to_disch':
        time_query = """
                     and (n.chartdate >= f.admittime OR n.charttime >= f.admittime)
		     and (n.chartdate <= (f.dischtime) OR
                     n.charttime <= (f.dischtime))
                     """
    else:
        print 'Invalid time period specified; aborting'
        return

    t1 = time()
    sqluser = os.environ['USER']    # assume SQL username is same as OS username
    dbname = 'mimic'
    schema_name = 'mimiciii'
    con = psycopg2.connect(dbname=dbname, user=sqluser)
    cur = con.cursor()
    
    cur.execute('SET search_path to ' + schema_name)
    query = \
    """
    with first_icustays as (
    select distinct i.subject_id, i.hadm_id,
    i.icustay_id, i.intime, i.outtime, i.admittime, i.dischtime
      FROM icustay_detail i
      LEFT JOIN icustays s ON i.icustay_id = s.icustay_id
      WHERE s.first_careunit NOT like 'NICU'
      and i.hospstay_seq = 1
      and i.icustay_seq = 1
      and i.age >= 15
      and i.los_icu >= 0.5
    )
    select n.row_id, n.category, n.hadm_id, n.text, COALESCE(n.charttime, n.chartdate), f.admittime
    from noteevents n
    inner join first_icustays f on f.hadm_id = n.hadm_id
    where iserror IS NULL --this is null in mimic 1.4, rather than empty space
    and lower(trim(category, ' ')) in (%s)
    %s
    ;
    """ % (', '.join(genres), time_query)
    print 'Executing query'
    cur.execute(query)
    records = cur.fetchall()
    print 'Done executing query'
    print 'Fetching record ids'
    row_ids = [record[0] for record in records]
    categories = [record[1] for record in records]
    hadm_ids = [record[2] for record in records]
    notes = [record[3] for record in records]
    times = [record[4]-record[5] for record in records]
    data = pd.DataFrame(data={'row_id': row_ids, 'hadm_id': hadm_ids, 'category': categories, 
                                  'time': times, 'note': notes})
    print 'Done fetching record ids. Number of records : ', len(row_ids)

    print 'Dumping data'
    print 'Number of documents : ', len(row_ids)
    print 'Time : %3f' % (time() - t1)
    print 'Done building corpus for genres : ', genres
    return data


if __name__ == "__main__":
    categories = ["'physician'", "'nursing'", "'nursing/other'",
                  "'respiratory'", "'ecg'", "'radiology'", "'rehab services'",
                  "'nutrition'", "'pharmacy'", "'social work'",
                  "'case management'", "'general'", "'echo'", "'consult'"]
    time_period = 'admit_to_disch' # options: admit_to_24h_disch, admit_to_disch
    data = build_notes_dataframe(time_period, categories)
    data_path = os.path.join(output_dir, 'mimic_notes_dataframe_' + time_period + '.p')
    data.to_pickle(data_path)
