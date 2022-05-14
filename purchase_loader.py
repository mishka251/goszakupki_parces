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


def save_to_cache(cache_filename: str, region: str, chunk_index: int):
    cache = open(cache_filename, 'w')
    cache.write(region)
    cache.write('\n')
    cache.write(str(chunk_index))
    cache.close()


def retry(retry_count=5, default=None):
    def retry_decorator(function):
        def inner(*args, **kwargs):
            for i in range(retry_count):
                try:
                    return function(*args, **kwargs)
                except Exception as ex:
                    print(f'Попытка {i+1} из {retry_count} закончилась с ошибкой')
            return default
        return inner
    return retry_decorator


class PurchaseLoader:
    """
    Класс для загрузки xml с фтп
    Данные сохраняются в xml файлах для последующего анализа с помощью xml_parcer.py
    """

    def __init__(self):
        self.open_ftp()

    @retry()
    def open_ftp(self):
        self.ftp = ftplib.FTP('ftp.zakupki.gov.ru')
        self.ftp.login('free', 'free')

    @retry(default=[])
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

    @retry()
    def _save_xml_file(self, file: FileInfo, region_name: str) -> None:
        content: str = file.binary.decode('utf-8')
        tree = minidom.parseString(content)
        code: Optional[str] = get_okpd2_from_xml(tree)
        if code is None:
            return

        with orm.db_session:
            is_po = PoClass.get(code=code) is not None
        if not is_po:
            return
        save_file_to_db(tree, region_name)

    def get_region(self, region_name: str, cache_filename: str, last_chunk_index: Optional[int] = None) -> None:
        """
        Получение данных о закупках в регионе
        :param region_name: название региона
        :return:
        """
        self.ftp.cwd(f'/fcs_regions/{region_name}/notifications')

        try:
            line_chunks = self.get_specific_line_chunks(self.is_necessary)
        except:
            self.open_ftp()
            line_chunks = self.get_specific_line_chunks(self.is_necessary)

        length = len(line_chunks)
        if last_chunk_index is not None:
            line_chunks = line_chunks[last_chunk_index:]

        for _index, chunks in enumerate(line_chunks):
            index = _index if last_chunk_index is None else _index+ last_chunk_index
            file = self.get_file(chunks)
            if file is None:
                continue
            save_to_cache(cache_filename, region_name, index)

            xml_files = self.get_xml_files(file)

            for file in xml_files:
                self._save_xml_file(file, region_name)

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

    @retry()
    def get_file(self, line_chunks) -> FileInfo:
        name = line_chunks['name']

        binary_chunks = []
        try:
            self.ftp.retrbinary(f'RETR {name}', binary_chunks.append)
        except Exception:
            self.open_ftp()
            self.ftp.retrbinary(f'RETR {name}', binary_chunks.append)
        return FileInfo(name, b''.join(binary_chunks))

    def get_specific_line_chunks(self, condition) -> List:
        return [line_chunks for line_chunks in self.get_line_chunks() if condition(line_chunks)]

    def is_file(self, line_chunks):
        return line_chunks['type'] == '-'

    def is_zip(self, line_chunks):
        return line_chunks['name'].endswith('.zip')

    def is_necessary(self, line_chunks):
        return self.is_file(line_chunks) and self.is_zip(line_chunks)


def main():
    cache_filename = 'cache.txt'
    last_region: Optional[str] = None
    last_file: Optional[int] = None
    try:
        cache_read = open(cache_filename, 'r')
        last_region = cache_read.readline().strip()
        last_file = int(cache_read.readline())
        cache_read.close()
    except:
        pass
    loader = PurchaseLoader()
    with orm.db_session:
        regions: List[str] = list(orm.select(r.name for r in Region))
    if last_region is not None:
        loader.get_region(last_region, cache_filename, last_file)
        regions = list(filter(lambda region: region > last_region, regions))
    for region in regions:
        save_to_cache(cache_filename, region, 0)
        loader.get_region(region, cache_filename, last_file)


if __name__ == "__main__":
    main()
