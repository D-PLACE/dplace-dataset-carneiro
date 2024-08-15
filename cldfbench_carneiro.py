import re
import pathlib
import textwrap
import collections
import unicodedata

from cldfbench import Dataset as BaseDataset

from pydplace import DatasetWithSocieties
from pydplace.dataset import data_schema
from pycldf.sources import Sources

REFS = collections.Counter()


def iter_refs(s):
    s = s.replace(
        "Cook [?]: 226; Ellis 1831, vol. 3: 123; Cook [?]: 226",
        "Cook [?]: 226; Ellis 1831 (vol. 3): 123"
    )
    for k, v in {
        'Busia 13-14': 'Busia 1951: 13-14',
        'Rattray : 100,134': 'Rattray 1923: 100,134',
        'XXIX;XXXIII;': 'XXIX,XXXIII;',
        'Howel 1941:': 'Howell 1941:',
        'Howell [?], etc. all sources': 'Howell 1941; Howell 1944; Howell 1952a; Howell 1952b; Howell 1953',
        'Howell 1952:': 'Howell 1952a:',
        'Leinhardt 1954': 'Lienhardt 1954',
        'Evans-Pitchard': 'Evans-Pritchard',
        '(+) ?+B386:B402': '',
        'Pumphrey 1914': 'Pumphrey 1941',
        'Oyler 1918:': 'Oyler 1918a:',
        'Evans-Pritchard: inter alia;': 'Evans-Pritchard 1948; Evans-Pritchard 1951;',
        'Kane 1930 (Vol. 3) 733-735': 'Kane 1930 (Vol. 3): 733-735',
        'Basham 1960:': 'Basham 1963:',
        'Basham 1963; 112': 'Basham 1963: 112',
        '; ?:33': '',
        'Galiano & Valdés 1930:': 'Galiano & Valdés 1930:',
        'Du Chaillu 1889, Vol. 2:': 'Du Chaillu 1889 (Vol. 2):',
        'Du Chaillu 1889, Vol. 1:': 'Du Chaillu 1889 (Vol. 1):',
        'Du chaillu 1889, Vol. 1:': 'Du Chaillu 1889 (Vol. 1):',
        'Waterhpise 1901': 'Waterhouse 1901',
        'Suetonius 1957; 218': 'Suetonius 1957: 218',
        '; Frank [?]': '',
        'Frank [?]; ': '',
        'Cary and Haarhoff 1940': 'Cary & Haarhoff 1940',
        '; Adams 1801: 310': '',
        'Carcopino 176,187': 'Carcopino 1956: 176,187',
        'Eells 1887:': 'Eells 1887b:',
        'Eels 1887:': 'Eells 1887b:',
        'Eels 1879': 'Eells 1879',
        'Mill 1926': 'Mills 1926',
        'Goltz 1929': 'Glotz 1929',
        'Goltz 1967': 'Glotz 1967',
        'Robertson [?]': 'Robertson 1875',
        '; Tyler 1965: 197': '',
        '; Smith 1831: 129': '',
        'Swanton 1928:': 'Swanton 1928a:',
        'Swanton 1928s': 'Swanton 1928a',
        'Morgan 1901, Vol. 1': 'Morgan 1901 (Vol. 1)',
        'Morgan 1901, Vol. 2': 'Morgan 1901 (Vol. 2)',
        'Wilkinson 1879, Vol. 2': 'Wilkinson 1879 (Vol. 2)',
        'Wilkinson, Vol. 2': 'Wilkinson 1879 (Vol. 2)',
        'Wilkinson 1879, Vol. 1': 'Wilkinson 1879 (Vol. 1)',
        'Wilkinson, Vol. 1': 'Wilkinson 1879 (Vol. 1)',
        'Spencer [?]: col. 200': '',
        'Spencer [?]: col. 16': '',
        'Spencer [?]: col. 14,24': '',
        'Myers 1894': 'Myer 1894',
        'Schweinfurth 1874, vol. I:': 'Schweinfurth 1874 (vol. I):',
        'Schweinfurth 1874, vol I:': 'Schweinfurth 1874 (vol. I):',
        'Schweinfurth 1874, vol. II': 'Schweinfurth 1874 (vol. II)',
        'Schweinfurth 1874 ,vol. II': 'Schweinfurth 1874 (vol. II)',
        'Schweinfurth 1874, vol II': 'Schweinfurth 1874 (vol. II)',
        'Lagae & Vanden Plas 1921, vol. 18': 'Lagae & Vanden Plas 1921 (vol. 18)',
        'Lagae & Vanden Plas 1921, vol. 6': 'Lagae & Vanden Plas 1921 (vol. 6)',
        'Lagae & Vanden Plas 1921, vol.18': 'Lagae & Vanden Plas 1921 (vol. 18)',
        'Evans-Pritchard 1957:': 'Evans-Pritchard 1957a:',
        'Ellis 1831, vol. 1': 'Ellis 1831 (vol. 1)',
        'Ellis 1831, Vol 1': 'Ellis 1831 (vol. 1)',
        'Ellis 1831, vol. 2': 'Ellis 1831 (vol. 2)',
        'Ellis 1831, vol. 3': 'Ellis 1831 (vol. 3)',
        'Williamson 1924, vol. 1': 'Williamson 1924 (vol. 1)',
        'Williamson 1924, vol. 2': 'Williamson 1924 (vol. 2)',
        'Williamson 1924, vol. 3': 'Williamson 1924 (vol. 3)',
        'Handy [1930?]': 'Handy 1930',
        'Briket-Smith 1930': 'Birket-Smith 1930',
        'Gould ??': '',
        'Gould 1976': '',
    }.items():
        s = s.replace(k, v)
    chunks, agg = [], ''
    for ref in s.split(';'):
        if agg and re.match('\s*[0-9]', ref):
            agg += ';' + ref
        else:
            if agg:
                chunks.append(agg.strip())
            agg = ref
    if agg:
        chunks.append(agg.strip())

    vol_pattern = re.compile(r'\s*(?P<vol>\([Vv](ol)?\.\s*[0-9I]+\))')
    for ref in chunks:
        ref = ref.strip()
        if ref.endswith(' passim') and (':' not in ref):
            yield ref.replace(' passim', '').strip(), 'passim'
            continue
        m = vol_pattern.search(ref)
        if m:
            ref = re.sub(vol_pattern, '', ref)
            vol = m.group('vol')
        else:
            vol = None
        ref, _, pages = ref.partition(':')
        if pages and vol:
            pages = '{}: {}'.format(vol, pages)
        REFS.update([ref.strip()])
        yield ref.strip(), pages or vol or None


class Dataset(DatasetWithSocieties):
    dir = pathlib.Path(__file__).parent
    id = "dplace-dataset-carneiro"

    def mkid(self, local):
        return '{}_{}'.format(self.id.split('-')[-1].upper(), local)

    def cmd_download(self, args):
        for p in self.raw_dir.joinpath('societies').glob('*.xlsx'):
            self.raw_dir.joinpath('societies').xlsx2csv(p.name)
        for p in self.raw_dir.joinpath('traits').glob('*.xlsx'):
            self.raw_dir.joinpath('traits').xlsx2csv(p.name)

    def cmd_makecldf(self, args):
        data_schema(args.writer.cldf)
        self.schema(args.writer.cldf)

        # Add data
        args.writer.cldf.sources = Sources.from_file(self.raw_dir / 'sources.bib')
        src_map = {}
        for r in self.raw_dir.read_csv('References_notes.csv', dicts=True, encoding='latin1'):
            r = {k: '' if v == 'NA' else v for k, v in r.items()}
            if r['D-PLACE']:
                if r['D-PLACE'].split()[0].split('/')[-1] != r['Bibtext'].replace(' - DUPLICATE', ''):
                    print(r)
            src_map[unicodedata.normalize('NFC', r['Reference'])] = r['Bibtext'].replace(' - DUPLICATE', '')
        fixkey = {
            'Seligman 1910': 'Seligmann 1910',
            'Métreaux 1948': 'Métraux 1948',
            'Métreaux 1928': 'Métraux 1928',
            'Selingman & Selingman 1932': 'Seligman & Seligman 1932',
            'Santandrea 1944': 'Santandrea 1944-1945',
            'Santandrea 1945': 'Santandrea 1944-1945',
            'Richard 1928': 'Reichard 1928',
            'Richard 1950': 'Reichard 1950',
            'Roth 1980': 'Roth 1890',
            'Bodwich 1873': 'Bowdich 1873',
            'Rattray1927': 'Rattray 1927',
            'Rattray 1937': 'Rattray 1927',
            'MArsden 1811': 'Marsden 1811',
        }
        skip = {
            'Gould 1967',
        }
        focal_years = {}

        for row in self.raw_dir.read_csv('societies.csv', encoding='latin1', dicts=True):
            row['ID'] = self.mkid(row['ID'])
            if not row['Latitude']:
                continue
            focal_years[row['ID']] = row['main_focal_year'] or None
            self.add_society(args.writer, **{k: v.strip() for k, v in row.items()})
        for row in self.raw_dir.read_csv('traits.csv', encoding='latin1', dicts=True):
            row = {k: v.strip() for k, v in row.items()}
            rid = self.mkid(row['Trait_ID_6th'])
            # Trait_ID_6th,Category,Trait_description
            if row['Trait_ID_6th']:
                args.writer.objects['ParameterTable'].append(dict(
                    ID=rid,
                    Name=row['Trait_name'],
                    Description=row['Trait_description'],
                    category=[row['Category']],
                    type='Categorical',
                    ColumnSpec=None,
                ))
                for desc, code in [('absent', '0'), ('present', '1')]:
                    args.writer.objects['CodeTable'].append(dict(
                        ID='{}-{}'.format(rid, code),
                        Var_ID=rid,
                        Name=desc,
                        Description=desc,
                        ord=int(code),
                    ))
        i = 0
        for p in self.raw_dir.joinpath('societies').glob('*.csv'):
            for row in p.parent.read_csv(p.name, dicts=True):
                row = {k: v.strip() for k, v in row.items()}
                if not row['Trait_ID_6th']:
                    continue
                rid = self.mkid(row['Trait_ID_6th'])
                i += 1
                source = []
                for key, pages in iter_refs(row['Reference']):
                    # lookup bibtex key
                    if key in skip:
                        continue
                    src = src_map[unicodedata.normalize('NFC', fixkey.get(key, key))]
                    if pages:
                        src += '[{}]'.format(pages.strip().replace(';', ','))
                    source.append(src)
                sid = self.mkid(p.stem.split('_')[0])
                args.writer.objects['ValueTable'].append(dict(
                    ID=str(i + 1),
                    Var_ID=rid,
                    Code_ID='{}-{}'.format(rid, row['Trait_presence']),
                    Soc_ID=sid,
                    Value=row['Trait_presence'],
                    Comment=row['Original_notes'],
                    Source=source,
                    admin_comment=row['Comments'],
                    year=int(focal_years[sid]) if focal_years[sid] else None,
                ))
        self.local_makecldf(args)
