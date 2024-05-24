import collections
import pathlib
import textwrap

from cldfbench import Dataset as BaseDataset

from pydplace import DatasetWithSocieties
from pydplace.dataset import data_schema

REFS = collections.Counter()


def iter_refs(s):
    for ref in s.split(';'):
        ref = ref.strip()
        ref, _, pages = ref.partition(':')
        REFS.update([ref.strip()])
        if pages:
            yield '{}[{}]'.format(ref.strip(), pages.strip())
        else:
            yield ref.strip()


class Dataset(DatasetWithSocieties):
    dir = pathlib.Path(__file__).parent
    id = "carneiro"

    def cmd_download(self, args):
        for p in self.raw_dir.joinpath('societies').glob('*.xlsx'):
            self.raw_dir.joinpath('societies').xlsx2csv(p.name)
        for p in self.raw_dir.joinpath('traits').glob('*.xlsx'):
            self.raw_dir.joinpath('traits').xlsx2csv(p.name)

    def cmd_makecldf(self, args):
        data_schema(args.writer.cldf)
        self.schema(args.writer.cldf)

        #
        # Add data

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
                args.writer.objects['ValueTable'].append(dict(
                    ID=str(i + 1),
                    Var_ID=row['Trait_ID_6th'],
                    Code_ID='{}-{}'.format(row['Trait_ID_6th'], row['Trait_presence']),
                    Soc_ID=p.stem.split('_')[0],
                    Value=row['Trait_presence'],
                    Comment=row['Original_notes'],
                    Source=list(iter_refs(row['Reference'])),
                    admin_comment=row['Comments'],
                ))
        self.local_makecldf(args)

        for k, v in REFS.most_common():
            print(k)
        print(len(REFS))
