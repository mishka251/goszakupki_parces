import ftplib
import io
import zipfile

from database import orm, PoClass, Region
from xml_parcer import save_file_to_db, get_okpd2_from_xml
from typing import List, Optional, Collection
from xml.dom import minidom


class FileInfo(object):
    name: str
    binary: bytes

    def __init__(self, name: str, binary: bytes):
        self.name = name
        self.binary = binary

    def __str__(self):
        return self.name


class PurchaseLoader:
    """
    Класс для загрузки xml с фтп
    Данные сохраняются в xml файлах для последующего анализа с помощью xml_parcer.py
    """

    def __init__(self):
        self.ftp = ftplib.FTP('ftp.zakupki.gov.ru')
        self.ftp.login('free', 'free')

    def get_xml_files(self, file: FileInfo) -> List[FileInfo]:
        excluded_types: List[str] = ['.sig']

        xml_files: List[FileInfo] = []

        if file.name.endswith('.xml'):
            xml_files.append(file)
        elif file.name.endswith(".zip"):
            zip_file = zipfile.ZipFile(io.BytesIO(file.binary))

            for filename in zip_file.namelist():
                file = zip_file.read(filename)
                xml_files.extend(self.get_xml_files(FileInfo(filename, file)))
        elif any(file.name.endswith(exclude) for exclude in excluded_types):
            return []
        else:
            print(f"Unknown file type {file.name}")
            return []

        return xml_files

    def get_region(self, region_name: str) -> None:
        """
        Получение данных о закупках в регионе
        :param region_name: название региона
        :return:
        """
        self.ftp.cwd(f'/fcs_regions/{region_name}/notifications')

        line_chunks = self.get_specific_line_chunks(self.is_necessary)

        length = len(line_chunks)

        for index, chunks in enumerate(line_chunks):
            file = self.get_file(chunks)
            xml_files = self.get_xml_files(file)
            for file in xml_files:
                content: str = file.binary.decode('utf-8')
                tree = minidom.parseString(content)
                code: Optional[str] = get_okpd2_from_xml(tree)
                if code is None:
                    continue

                with orm.db_session:
                    is_po = PoClass.get(code=code) is not None
                if not is_po:
                    continue
                save_file_to_db(tree, region_name)

            print(f"{region_name} - {int(((index + 1) / length) * 100)}% loaded")

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

    def get_file(self, line_chunks) -> FileInfo:
        name = line_chunks['name']

        binary_chunks = []
        self.ftp.retrbinary(f'RETR {name}', binary_chunks.append)
        return FileInfo(name, b''.join(binary_chunks))

    def get_specific_line_chunks(self, condition):
        return [line_chunks for line_chunks in self.get_line_chunks() if condition(line_chunks)]

    def is_file(self, line_chunks):
        return line_chunks['type'] == '-'

    def is_zip(self, line_chunks):
        return line_chunks['name'].endswith('.zip')

    def is_necessary(self, line_chunks):
        return self.is_file(line_chunks) and self.is_zip(line_chunks)


def main():
    loader = PurchaseLoader()
    with orm.db_session:
        regions: List[str] = list(orm.select(r.name for r in Region))
    for region in regions:
        loader.get_region(region)


if __name__ == "__main__":
    main()
