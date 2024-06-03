import re
import collections
import pathlib
import textwrap

from cldfbench import Dataset as BaseDataset

from pydplace import DatasetWithSocieties
from pydplace.dataset import data_schema

REFS = collections.Counter()


def iter_refs(s):
    for k, v in {
        'Busia 13-14': 'Busia 1951: 13-14',
        'Rattray : 100,134': 'Rattray 1923: 100,134',
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
    id = "carneiro"

    def cmd_download(self, args):
        for p in self.raw_dir.joinpath('societies').glob('*.xlsx'):
            self.raw_dir.joinpath('societies').xlsx2csv(p.name)
        for p in self.raw_dir.joinpath('traits').glob('*.xlsx'):
            self.raw_dir.joinpath('traits').xlsx2csv(p.name)

    def cmd_makecldf(self, args):
        from pycldf.sources import Sources
        data_schema(args.writer.cldf)
        self.schema(args.writer.cldf)

        #
        # Add data
        #
        args.writer.cldf.sources = Sources.from_file(self.raw_dir / 'sources.bib')
        src_map = {
            r['Reference']: r['Bibtext'] for r in
            self.raw_dir.read_csv('References_2024_05_28.csv', dicts=True, encoding='latin1')}
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
        }

        # ID,Name,Latitude,Longitude,Glottocode,Name_and_ID_in_source,xd_id,main_focal_year,HRAF_name_ID,HRAF_ID ,region,comment,,
        for row in self.raw_dir.read_csv('Carneiro_societies.csv', encoding='latin1', dicts=True):
            if not row['Latitude']:
                continue
            self.add_society(args.writer, **{k: v.strip() for k, v in row.items()})
        for row in self.raw_dir.joinpath('traits').read_csv('6th_edition_traits.Sheet1.csv', dicts=True):
            row = {k: v.strip() for k, v in row.items()}
            # Trait_ID_6th,Category,Trait_description
            if row['Trait_ID_6th']:
                args.writer.objects['ParameterTable'].append(dict(
                    ID=row['Trait_ID_6th'],
                    Name=textwrap.shorten(row['Trait_description'], 50),
                    Description=row['Trait_description'],
                    category=row['Category'],
                    type='Categorical',
                    ColumnSpec=None,
                ))
                for desc, code in [('absent', '0'), ('present', '1')]:
                    args.writer.objects['CodeTable'].append(dict(
                        ID='{}-{}'.format(row['Trait_ID_6th'], code),
                        Var_ID=row['Trait_ID_6th'],
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
                # Trait_ID_6th,Trait_presence,Reference,Original_notes,Comments
                i += 1
                source = []
                for key, pages in iter_refs(row['Reference']):
                    # lookup bibtex key
                    src = src_map[fixkey.get(key, key)]
                    if pages:
                        src += '[{}]'.format(pages.strip().replace(';', ','))
                    source.append(src)
                args.writer.objects['ValueTable'].append(dict(
                    ID=str(i + 1),
                    Var_ID=row['Trait_ID_6th'],
                    Code_ID='{}-{}'.format(row['Trait_ID_6th'], row['Trait_presence']),
                    Soc_ID=p.stem.split('_')[0],
                    Value=row['Trait_presence'],
                    Comment=row['Original_notes'],
                    Source=source,
                    admin_comment=row['Comments'],
                ))
        self.local_makecldf(args)
