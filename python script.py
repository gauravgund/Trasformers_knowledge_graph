import spacy
import spacy_transformers
import torch
import pandas as pd

ner_model = r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\ner_model\content\outputs\model-best'

model= spacy.load(ner_model)

text = [
'''Qualifications
- A thorough understanding of C# and .NET Core
- Knowledge of good database design and usage
- An understanding of NoSQL principles
- Excellent problem solving and critical thinking skills
- Curious about new technologies
- Experience building cloud hosted, scalable web services
- Azure experience is a plus
Requirements
- Bachelor's degree in Computer Science or related field
(Equivalent experience can substitute for earned educational qualifications)
- Minimum 4 years experience with C# and .NET
- Minimum 4 years overall experience in developing commercial software
'''
]

for doc in model.pipe(text, disable=["tagger", "parser"]):
    print([(ent.text, ent.label_) for ent in doc.ents])


import os
os.chdir(r'C:\Users\gaurav.gund\rel_component')
from scripts.rel_pipe import make_relation_extractor, score_relations
from scripts.rel_model import create_relation_model, create_classification_layer, create_instances, create_tensors

nlp2 = spacy.load(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\rel_model\content\rel_component\training\model-best')
nlp2.add_pipe('sentencizer')

for name, proc in nlp2.pipeline:
          doc = proc(doc)



      

def relation_ext(rel_dict):
    if rel_dict['DEGREE_IN'] > rel_dict['EXPERIENCE_IN']:
        return('DEGREE_IN')
    else:
        return('EXPERIENCE_IN')

a_txt=[]
b_txt=[]
c_txt=[]


for value, rel_dict in doc._.rel.items():
  for sent in doc.sents:
    for e in sent.ents:
      for b in sent.ents:
        if e.start == value[0] and b.start == value[1]:
            if rel_dict['EXPERIENCE_IN']> 0.9:
                a_txt.append(e.text)
                b_txt.append(b.text)
                c_txt.append('EXPERIENCE_IN')
            if rel_dict['DEGREE_IN']> 0.9:
                a_txt.append(e.text)
                b_txt.append(b.text)
                c_txt.append('DEGREE_IN')

len(a_txt), len(b_txt), len(c_txt)

import ast

with open(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\relation_extraction_data.txt', 'r') as f:
    mylist = ast.literal_eval(f.read())
rel_data = [mylist[i]['document'] for i in range(len(mylist))]


a_txt=[]
b_txt=[]
c_txt=[]
for doc in model.pipe(rel_data, disable=["tagger", "parser"]):
    for name, proc in nlp2.pipeline:
          doc = proc(doc)
          
    for value, rel_dict in doc._.rel.items():
        for sent in doc.sents:
            for e in sent.ents:
                for b in sent.ents:
                    if e.start == value[0] and b.start == value[1]:
                        if rel_dict['EXPERIENCE_IN']> 0.9:
                            a_txt.append(e.text)
                            b_txt.append(b.text)
                            c_txt.append('EXPERIENCE_IN')
                        if rel_dict['DEGREE_IN']> 0.9:
                            a_txt.append(e.text)
                            b_txt.append(b.text)
                            c_txt.append('DEGREE_IN')

len(a_txt), len(b_txt), len(c_txt)


df = pd.DataFrame({'ent1': a_txt, 'ent2': b_txt, 'relation': c_txt})

#df.to_csv(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\ent_relation_results.csv', index= False)
            
from neo4j import GraphDatabase, basic_auth
import pandas as pd


from neo4j import GraphDatabase, basic_auth


import pandas as pd
import time
rel_data = pd.read_csv(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\exp_rel.csv')

experience= rel_data[['experience']].drop_duplicates()
skill= rel_data[['skill']].drop_duplicates()
rel_df = rel_data[['experience', 'skill', 'relation']].drop_duplicates()


degree_data = pd.read_csv(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\degree_rel.csv')
degree_df = degree_data[['degree', 'subject','relation']].drop_duplicates()



class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


conn = Neo4jConnection(uri="bolt://3.82.117.207:7687", 
                       user="neo4j",              
                       pwd="cargo-equations-circuits")

def add_exp(experience):
    # Adds category nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (exp:Experience {exp_level:row.experience})
            RETURN count(*) as total_exp
            '''
    return conn.query(query, parameters = {'rows':experience.to_dict('records')})


def add_skill(skill):
    # Adds category nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (sk:Skill {skill_name:row.skill})
            RETURN count(*) as total_skill
            '''
    return conn.query(query, parameters = {'rows':skill.to_dict('records')})


def insert_data(query, rows, batch_size = 10000):
    # Function to handle the updating the Neo4j database in batch mode.
    
    total = 0
    batch = 0
    start = time.time()
    result = None
    
    while batch * batch_size < len(rows):

        res = conn.query(query, 
                         parameters = {'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')})
        total += res[0]['total']
        batch += 1
        result = {"total":total, 
                  "batches":batch, 
                  "time":time.time()-start}
        print(result)
        
    return result


def add_papers(rows, batch_size=5000):
   query = '''
   UNWIND $rows as row
   
   MERGE (exp:Experience {exp_level:row.experience})
   MERGE (sk:Skill {skill_name:row.skill})
   
  // UNWIND row.skill AS skill1
   //UNWIND row.experience AS experience1
   //UNWIND row.relation AS exp1
   
   WITH row, sk, exp
   
   //MATCH (exp:Experience {exp_level:experience1})
   //MATCH (sk:Skill {skill_name:skill1})

   MERGE (exp)-[rel1:Experience_In {rel_type:'Experience_In'}]->(sk)
 
   
   RETURN count(*) as total
   '''
 
   return insert_data(query, rows, batch_size)


neo4j_query("""
MATCH (n) DETACH DELETE n;
""")

query = '''
   UNWIND $rows as row
   
   MERGE (exp:Experience {exp_level:row.experience})
   MERGE (sk:Skill {skill_name:row.skill})
   
   WITH row, sk, exp
   MERGE (exp)-[rel1:Experience_In {rel_type:'Experience_In'}]->(sk)
  
   
   RETURN count(*) as total
    '''


conn.query(query,  parameters = {'rows':rel_df.to_dict('records')})


query1 = '''
   UNWIND $rows as row
   
   MERGE (deg:Degree {deg_level:row.degree})
   MERGE (sub:Subject {subject_name:row.subject})
   
   WITH row, sub, deg
   MERGE (deg)-[rel2:Degree_In {rel_type2:'Degree_In'}]->(sub)
  
   
   RETURN count(*) as total
    '''


conn.query(query1,  parameters = {'rows':degree_df.to_dict('records')})


########## data extraction based on knowledge graph ######################

query3 = '''

MATCH (p1:Experience)
WHERE p1.exp_level contains '3'
RETURN count(p1) AS COUNT, COLLECT(p1.exp_level) AS EXP_LEVEL_3;

'''

result = conn.query(query3)

df_test = pd.DataFrame(result, columns= ['COUNT', 'EXP_LEV_3'])

query4 = '''

MATCH (p1:Experience)-[rel1:Experience_In]->(p2:Skill)
WHERE (p1.exp_level contains '3' ) OR (p2.skill_name contains 'Linux')

RETURN p1.exp_level AS exp_level, p2.skill_name AS skill_name
'''

result = conn.query(query4)
result
df_test = pd.DataFrame(result, columns= ['exp', 'skill'])








##############################################################################################################




import os
os.chdir(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph')


#Create elements from exp_rel
# neo4j_query("""
# LOAD CSV WITH HEADERS FROM "https://github.com/gauravgund/knowledge-graph/blob/main/exp_rel.csv" AS csvLine 
# CREATE (exp:Experience {id:csvLine.id, exp_level:csvLine.experience}) 
# CREATE (sk:Skill {id:csvLine.id, skill_name:csvLine.skill})

# MATCH (exp:Experience {id:csvLine.id}), (sk:Skill {id:csvLine.id})

# CREATE (exp)-[rel1:Experience_rel {rel_name:"EXPERIENCE_IN"}]->(sk)
# RETURN exp, sk, rel1
# LIMIT 2;
#  """)



####################### using py2neo #########################################

from py2neo import Graph
import pandas as pd
rel_data = pd.read_csv(r'C:\Users\gaurav.gund\OneDrive - Grant Thornton Advisory Private Limited\Documents\d_drive\C Drive Backup 2021\Desktop\hackathon\knowledge_graph\exp_rel.csv')




graph = Graph(  "bolt://54.166.68.167:7687",user = "neo4j",password =  "swimmers-orange-commitment")
tx = graph.begin()


rel_data1= rel_data.head(10)

for index, row in rel_data1.iterrows():
    tx.evaluate('''
       MERGE (exp:Experience {exp_level:$p1})
       MERGE (sk:Skill {skill_name:$p2})
       MERGE (exp)-[rel1:Experience_rel {rel_name:"EXPERIENCE_IN"}]->(sk)
       ''', parameters = {'p1':row['experience'], 'p2': row['skill']})
tx.commit()




