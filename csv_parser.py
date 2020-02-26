from database import Classifier, PoClass, orm, Region
import os
import pandas as pd

classifier_folder: str = "./klassifikatory"
classifiers_file: str = "klassifikatory.csv"
regions_file: str = "./regions.csv"

with orm.db_session:
    # парсим регионы
    regions_info = pd.read_csv(regions_file, names=['name', 'rus'],
                               encoding='cp1251', delimiter=';')

    for _, region_row in regions_info.iterrows():
        region = Region.get(name=region_row['name']) or Region(name=region_row['name'])
        region.readable_name = region_row['rus']

    # парсим классы ПО
    classes_data = pd.read_csv(os.path.join(classifier_folder, classifiers_file), names=['file', 'name'],
                               encoding='cp1251', delimiter=';')
    for _, class_row in classes_data.iterrows():
        filename = class_row['file']
        name = str(class_row['name'])
        classifier = Classifier.get(name=name) or Classifier(name=name)
        # и для каждого классы - коды
        dfs = pd.read_csv(f"{classifier_folder}/{filename}", names=["code"], encoding='cp1251')
        for index, row in dfs.iterrows():
            code = str(row['code'])
            po_class = PoClass.get(code=code) or PoClass(code=code)
            po_class.classifier.add(classifier)

print("all ok")
