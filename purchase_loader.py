import ftplib
import io
import zipfile
import lxml.etree
import lxml.cssselect
import time
import sys
import os


class PurchaseLoader:

    def __init__(self):
        self.ftp = ftplib.FTP('ftp.zakupki.gov.ru')
        self.ftp.login('free', 'free')
        self.okpd2s = self.get_okpd2s()


    def get_okpd2s(self):
        okpd2s = set()
        f = open('klassifikatory.csv', 'r')
        lines = f.read().split('\n')
        for line in lines:
            okpd2s.add(line.split(';')[0])
        return okpd2s


    def get_region_names(self):
        self.ftp.cwd('/fcs_regions')

        for chunks in self.get_line_chunks():
            if self.is_region(chunks):
                yield chunks['name']


    def get_region(self, region_name):
        self.ftp.cwd(f'/fcs_regions/{region_name}/notifications')

        line_chunks = self.get_specific_line_chunks(self.is_necessary)

        current = 1
        length = len(line_chunks)

        for chunks in line_chunks:
            file = self.get_file(chunks)

            zip_file = zipfile.ZipFile(io.BytesIO(file['binary']))

            xml_files = []

            for xml_filename in zip_file.namelist():
                xml_binary = zip_file.read(xml_filename)
                xml_file = lxml.etree.parse(io.BytesIO(xml_binary))

                okpd2_selector = lxml.cssselect.CSSSelector('ns2|purchaseObject > ns2|OKPD2',
                    namespaces={'ns2': 'http://zakupki.gov.ru/oos/types/1'})
                for okpd2_tag in okpd2_selector(xml_file):
                    if okpd2_tag[0].text in self.okpd2s:
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


    # --------------------


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


    def get_region_suffixes(self):
        return ['_Resp', '_kraj', '_obl', '_g', '_AO', '_Aobl']

    
    def get_region_without_suffixes(self):
        return ['Moskva', 'Sankt-Peterburg']


    def is_region(self, line_chunks):
        name = line_chunks['name']

        for sffx in self.get_region_suffixes():
            if name.endswith(sffx):
                return True
        for wsffx in self.get_region_without_suffixes():
            if name.endswith(wsffx):
                return True
        return False

    
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


def get_region_names(loader, args):
    f = open('region_names.txt', 'w')
    for region_name in loader.get_region_names():
        f.write(f'{region_name}\n')


def get_region(loader, args):
    if not os.path.isdir(f'downloads/{args}'):
        os.mkdir(f'downloads/{args}')
    f = open(f"downloads/{args}/state.txt", 'w')
    for file in loader.get_region(args):
        if len(file['xml_files']) > 0:
            if not os.path.isdir(f"downloads/{args}/{file['filename']}"):
                os.mkdir(f"downloads/{args}/{file['filename']}")
            for xml_file in file['xml_files']:
                f = open(f"downloads/{args}/{file['filename']}/{xml_file['name']}", 'wb')
                f.write(xml_file['binary'])
        f = open(f"downloads/{args}/state.txt", 'a')
        f.write(f"{int((file['current'] / file['length']) * 100)}% analyzed\n")


def main():
    loader = PurchaseLoader()

    method = sys.argv[1]
    args = sys.argv[2]

    {
        "get_region_names": get_region_names,
        "get_region": get_region
    }[method](loader, args)


if __name__ == "__main__":
    main()