import evaluate
from rouge import Rouge
import pymssql
from dotenv import load_dotenv

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE = os.getenv('db')
DB_HOST = os.getenv('host')
DB_USERNAME = os.getenv('user')
DB_PASSWORD = os.getenv('password')

def return_mssql_dict(sql):
        connection={
          'host': DB_HOST,
          'username': DB_USERNAME,
          'password': DB_PASSWORD,
          'db': DATABASE
        }
        con=pymssql.connect(connection['host'],connection['username'],connection['password'],connection['db'])
        # con = pymssql.connect(server, user, password, database_name)
        cur = con.cursor()
        cur.execute(sql)

        def return_dict_pair(row_item):
            return_dict = {}
            for column_name, row in zip(cur.description, row_item):
                return_dict[column_name[0]] = row
            return return_dict

        return_list = []
        for row in cur:
            row_item = return_dict_pair(row)
            return_list.append(row_item)
            
        con.commit()
        con.close()

        return return_list

def find_accuracy(id):
  property_id = id #34953 #98174 #55222 
  query = '''select b.category, a.bylaw_id, b.bylaw from property_bylaws a
            left join preset_bylaws b on a.bylaw_id=b.id and a.bylaw_version=b.version
            where  bylaw is not null and a.property_id={}  '''.format(property_id)
  query2 = "select category, custom_bylaw from property_bylaws_custom where property_id={}".format(property_id)

  # following the format => column name: value
  ref = return_mssql_dict(query) # id, category, custom_bylaw
  ref2 = return_mssql_dict(query2)

  manual = {}

  for i in ref:
    if i['category'] in manual:
      dup = False
      for j in manual[i['category']]:
        if j == i['bylaw']:
          dup == True
          break # skip duplicate
      
      if dup == False:
        manual[i['category']] += " " + i['bylaw'].lower()
    else:
      manual[i['category']] = i['bylaw'].lower()

  for i in ref2:
    if i['category'] in manual:
      dup = False
      for j in manual[i['category']]:
        if j == i['custom_bylaw']:
          dup == True
          break # skip duplicate
      
      if dup == False:
        manual[i['category']] += " " + i['custom_bylaw'].lower() 

    else:
      manual[i['category']] = i['custom_bylaw'].lower()

  # manual

  # print("Manual len: {0}".format(len(manual)))

  cat = [i for i in manual]
  cat_tuple = tuple(cat)
  # cat_tuple

  # print("Category len: {0}".format(len(cat_tuple)))

  # id, category, corpus_summary
  cands = return_mssql_dict("select category, corpus_summary from [property_corpus_summary] where property_id={0} and category in {1}".format(property_id, cat_tuple))
  # cands

  summary = {}
  for i in cands:
      summary[i['category']] = i['corpus_summary'].lower()

  # print(manual)

  # print(summary)

  """# ROUGE"""
  rouge = Rouge()
  metric = {}

  for i in cat:
    # print(i)
    if i in summary:
      # print(summary[i], manual[i])
      if summary[i] == '':
        metric[i] = None
      elif not any(c.isalpha() for c in summary[i]):
        metric[i] = 0
      else:
        score = rouge.get_scores(summary[i], manual[i]) #, avg=True
        metric[i] = round(score[0]['rouge-1']['f'], 5)

  # print(metric)

  """# METEOR
  https://huggingface.co/spaces/evaluate-metric/meteor
  """
  meteor = evaluate.load('meteor')
  metric2 = {}

  for i in cat:
    # print(i)
    if i in summary:
      # print(summary[i], manual[i])
      if summary[i] == '':
        metric2[i] = None
      elif not any(c.isalpha() for c in summary[i]):
        metric2[i] = 0
      else:
        scores = meteor.compute(predictions=[summary[i]], references=[manual[i]])
        metric2[i] = round(scores['meteor'], 5)

  # print(metric2)

  final_score = {}

  for i, j in zip(metric, metric2):
    # print(i, j)
    if metric[i] != None and metric2[i] != None :
      final_score[i] = round((metric[i] + metric2[i]) / 2 * 100, 3)
      query_update = "update [property_corpus_summary] set accuracy={0} where property_id={1} and category='{2}'".format(final_score[i], property_id, i) 
      print(query_update)
      return_mssql_dict(query_update)

  print(final_score)


# Write properties into a TEMP txt file => after finishing, save those to properties.txt
def save_prop(items):
  with open('items.txt', 'w') as filehandle:
      for listitem in items:
          filehandle.write('%s\n' % listitem['property_id'])

# Find properties that have bylaws tool ready
prop_query = '''select p.id as property_id
                from property p left join [order] o on o.property_id=p.id inner join [user] u on o.user_id = u.id 
                where p.bylaws_ready=1 and o.status in (3,4) and u.client_id != 1'''
properties = return_mssql_dict(prop_query)
# print(len(properties))

items = properties[100:101]
# print(items)
#TODO: Validate if each item exists in properties.txt
save_prop(items)
count = 0

for x in items:
  # print(x)
  prop_id = x['property_id']
  print("Property: {0} (i = {1})".format(prop_id, count))
  find_accuracy(prop_id)
  count += 1

# find_accuracy(34953)

"""It ends here"""