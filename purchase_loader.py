import ftplib
import io
import zipfile
import os

from database import Region, orm
from xml_parcer import save_file_to_db

class PurchaseLoader:
    """
    Класс для загрузки xml с фтп
    Данные сохраняются в xml файлах для последующего анализа с помощью xml_parcer.py
    """

    def __init__(self):
        self.ftp = ftplib.FTP('ftp.zakupki.gov.ru')
        self.ftp.login('free', 'free')


    def get_region(self, region_name: str):
        """
        Получение данных о закупках в регионе
        :param region_name: название региона
        :return:
        """
        self.ftp.cwd(f'/fcs_regions/{region_name}/notifications')

        line_chunks = self.get_specific_line_chunks(self.is_necessary)

        current = 1
        length = len(line_chunks)

        for chunks in line_chunks:
            file = self.get_file(chunks)
            xml_files = []
            if file['name'].endswith('.xml'):

                xml_files.append(file)
            if not file['name'].endswith(".zip"):
                continue

            zip_file = zipfile.ZipFile(io.BytesIO(file['binary']))

            for xml_filename in zip_file.namelist():
                if not xml_filename.endswith(".xml"):
                    continue
                xml_binary = zip_file.read(xml_filename)
                xml_files.append({
                    'name': xml_filename,
                    'binary': xml_binary
                })

            yield {
                'region_name': region_name,
                'current': current,
                'length': length,
                'filename': file['name'],
                'xml_files': xml_files
            }

            current += 1

    def get_lines(self):
        lines = []
        self.ftp.retrlines('LIST', lines.append)
        return lines

    def get_chunks(self, line):
        chunks = [chunk for chunk in line.split(' ') if chunk != '']
        return {
            'type': chunks[0][0],
            'date': f'{chunks[-4]} {chunks[-3]} {chunks[-2]}',
            'name': chunks[-1]
        }

    def get_line_chunks(self):
        return [self.get_chunks(line) for line in self.get_lines()]

    def get_file(self, line_chunks):
        name = line_chunks['name']

        binary_chunks = []
        self.ftp.retrbinary(f'RETR {name}', binary_chunks.append)
        return {
            'date': line_chunks['date'],
            'name': name,
            'binary': b''.join(binary_chunks)
        }

    def get_specific_line_chunks(self, condition):
        return [line_chunks for line_chunks in self.get_line_chunks() if condition(line_chunks)]

    def is_file(self, line_chunks):
        return line_chunks['type'] == '-'

    def is_zip(self, line_chunks):
        return line_chunks['name'].endswith('.zip')

    def is_necessary(self, line_chunks):
        return self.is_file(line_chunks) and self.is_zip(line_chunks)


def get_region(loader: PurchaseLoader, region_name: str):
    """
    Загрузка региона
    из старого кода
    :param loader: лоадер
    :param region_name: назваине региона
    :return:
    """
    if not os.path.isdir(f'downloads/{region_name}'):
        os.mkdir(f'downloads/{region_name}')

    for file in loader.get_region(region_name):
        if len(file['xml_files']) > 0:
            # if not os.path.isdir(f"downloads/{region_name}/{file['filename']}"):
            #     os.mkdir(f"downloads/{region_name}/{file['filename']}")
            for xml_file in file['xml_files']:
                f = open(f"tmp_file", 'wb')
                f.write(xml_file['binary'])
                f.close()
                save_file_to_db(f"tmp_file", region_name)

        print(f"{region_name} - {int((file['current'] / file['length']) * 100)}% loaded")


def main():
    loader = PurchaseLoader()
    with orm.db_session:
        for region in orm.select(r.name for r in Region):
            get_region(loader, region)


if __name__ == "__main__":
    main()
