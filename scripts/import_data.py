import pdfplumber
from pathlib import Path
import pandas as pd
import unicodedata
import os
from neo4j import GraphDatabase
import re

RESSOURCES_FOLDER = "ressources/"

CLASS_TREE_PDF = "FdD_ACC_*.pdf"
ORDINARY_TECHNIQUES_PDF = "FdD_GTO_*.pdf"


class Settings:
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://0.0.0.0:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

settings = Settings()

driver = GraphDatabase.driver(
    settings.NEO4J_URI,
    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
)

def get_session():
    return driver.session()

def open_ressource_file(file_name):
    ctfs = [file for file in Path(RESSOURCES_FOLDER).rglob(file_name) if file.is_file()]
    if len(ctfs) != 1:
        print("Error cannot find class tree file")
        exit(1)
    ctf = ctfs[0]
    return pdfplumber.open(ctf)

#Parsing de l'arbre des classes
def parse_class_tree():

    class_dict = dict()
    relation_dict = dict()

    Label = ["BasicClass","AdvancedClass","SupremeClass"]

    def to_id(name):
        return ''.join(
            c for c in unicodedata.normalize('NFD', name.lower())
            if unicodedata.category(c) != 'Mn'
        ).replace(' ', '').replace("’",'')
        

    def parse_table_column(serie):
        trim_serie = serie[serie.notnull()]
        label_special = Label[trim_serie.name]
        for classes in trim_serie:
            for classe in classes.split(' | '):
                id = to_id(classe)
                try :
                    class_dict[id]
                except:
                    class_dict[id]=f"({id}:Class:{label_special} {{name:'{classe}'}})"

    def link_table_line(serie,base):
        previous = base
        for classes in serie[serie.notnull()]:
            for classe in classes.split(" | "):
                clean_classe = to_id(classe)
                try:
                    relation_dict[f"{previous}-{clean_classe}"]
                except:
                    relation_dict[f"{previous}-{clean_classe}"] = f"({previous})-[:Evolve]->({to_id(classe)})"
            previous = to_id(classes)

    with open_ressource_file(CLASS_TREE_PDF) as pdf:
        for page_id in range(2,len(pdf.pages)):
            page = pdf.pages[page_id]
            for table in page.extract_tables():
                table_pd = pd.DataFrame(table)
                table_pd.apply(parse_table_column ,axis=0)

                base_class = to_id(table_pd.iloc[0,0])
                table_pd.iloc[:,1:].apply(lambda x: link_table_line(x,base_class),axis=1)

    query = f"CREATE {','.join(class_dict.values())},{','.join(relation_dict.values())}"
    get_session().run(query)

def parse_ordinary_techniques():
    TECHNIQUE_HEADER = r"(?P<name>.+?)\s*-\s*(?P<type>.+?)\s*-\s*(?P<energy>\d+?) PE\s*-\s*(?P<slot>\d+?) PT"

    tech_list= list()

    def save_n_tech(n_tech,n_tech_desc):         
        if n_tech is not None:
            n_tech["desc"]=" ".join(n_tech_desc)
            tech_list.append(n_tech)

    def transform_to_neo4j(skill_dict):
        values = ','.join([ f"{key}:'{skill_dict[key]}'" for key in skill_dict ])
        return f"(:Skill:OrdinarySkill {{{values}}})"

    with open_ressource_file(ORDINARY_TECHNIQUES_PDF) as pdf:
        for page_id in range(3,len(pdf.pages)):
            page = pdf.pages[page_id]
            n_tech = None
            n_tech_desc = []
            for line in page.extract_text().split("\n")[:-1]:
                match = re.fullmatch(TECHNIQUE_HEADER,line)
                if (match is not None):
                    save_n_tech(n_tech,n_tech_desc)
                    n_tech = match.groupdict()
                    n_tech_desc=[]
                else:
                    n_tech_desc.append(line)
            save_n_tech(n_tech,n_tech_desc)
    
    #print(transform_to_neo4j(tech_list[0]))

    query = f"CREATE {','.join(map(transform_to_neo4j,tech_list))}"  
    print(query)
    get_session().run(query)

def main():
    #parse_class_tree()
    print("MAIN")
    parse_ordinary_techniques()


main()