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
    chunks, agg = [], ''
    for ref in s.split(';'):
        if agg and re.match(r'\s*[0-9]', ref):
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
            yield unicodedata.normalize('NFC', ref.replace(' passim', '').strip()), 'passim'
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
        yield unicodedata.normalize('NFC', ref.strip()), pages or vol or None


class Dataset(DatasetWithSocieties):
    dir = pathlib.Path(__file__).parent
    id = "dplace-dataset-carneiro6"

    def mkid(self, local):
        return '{}_{}'.format('CARNEIRO6', local)

    @property
    def raw(self):
        return self.raw_dir / '6TH_EDITION'

    def cmd_download(self, args):
        for p in self.raw.joinpath('societies').glob('*.xlsx'):
            self.raw.joinpath('societies').xlsx2csv(p.name)
        for p in self.raw.glob('*.xlsx'):
            self.raw.xlsx2csv(p.name)

    def iter_sources(self):
        # Add data
        # Read society association from bibfile: lines starting with
        # CARNEIRO6...
        # are heading the related bib entries. Note that there are two duplicates, possibly the same
        # source related to two societies.

        bibid_pattern = re.compile(r'@[a-z]+\{(?P<bibid>[^,]+),')

        chunk, bibids, soc = [], [], None
        for line in self.raw.joinpath(
                '6th_edition_sources.bib').read_text(encoding='utf8').split('\n'):
            if line.startswith('CARNEIRO6'):
                assert re.match(r'CARNEIRO6_[0-9]{3}_', line)
                if chunk:
                    assert soc
                    yield soc, '\n'.join(chunk), bibids
                chunk, bibids, soc = [], [], '_'.join(line.split('_')[:2])
            else:
                m = bibid_pattern.fullmatch(line.strip())
                if m:
                    bibids.append(m.group('bibid'))
                chunk.append(line)
        assert chunk
        yield soc, unicodedata.normalize('NFC', '\n'.join(chunk)), bibids

    def cmd_makecldf(self, args):
        data_schema(args.writer.cldf)
        self.schema(args.writer.cldf)

        # FIXME: We need the keys, because those are referenced from the individual sheets!
        soc2bib = {}
        for soc, bibtex, bibids in self.iter_sources():
            args.writer.cldf.add_sources(bibtex)
            soc2bib[soc] = bibids  # Make sure that all items in this dict are claimed!

        key2bib = {}
        for rec in args.writer.cldf.sources:
            key2bib[rec['key']] = rec.id
            key2bib[rec['key'].replace('(', '').replace(')', '')] = rec.id
            key2bib[rec['key'].replace(' and ', ' & ')] = rec.id
            key2bib[rec['key'].replace(' and ', ' & ').replace('(', '').replace(')', '')] = rec.id
            key2bib[rec['key'].replace('(nd)', '[?]')] = rec.id

        focal_years = {}
        socids = set()
        for row in self.raw.read_csv(
                '6th_edition_societies.6theditionsocieties.csv', dicts=True):
            #ID, Name, Latitude, Longitude, Glottocode, Name_and_ID_in_source, xd_id, main_focal_year, HRAF_name_ID, HRAF_ID, region, comment
            assert row['Latitude']
            socids.add(row['ID'])
            focal_years[row['ID']] = row['main_focal_year'] or None
            self.add_society(args.writer, **{k: v.strip() for k, v in row.items()})
        assert set(soc2bib).issubset(socids)
        #Trait_ID_6th, Category, Trait_name, Trait_description
        for row in self.raw.read_csv('6th_edition_traits.Sheet1.csv', dicts=True):
            row = {k: v.strip() for k, v in row.items()}
            rid = row['Trait_ID_6th']
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
        for p in self.raw.joinpath('societies').glob('*.csv'):
            #Trait_ID_6th,Trait_presence,Reference,Original_notes,Comments
            for row in p.parent.read_csv(p.name, dicts=True):
                row = {k: v.strip() for k, v in row.items()}
                if not row['Trait_ID_6th']:
                    continue
                rid = self.mkid(row['Trait_ID_6th'])
                i += 1
                source = []
                for key, pages in iter_refs(row['Reference']):
                    key = {
                        'Gould 1967': 'Gould pers. comm. (1967)',
                        'Roth 1980': 'Roth 1890',
                        'Selingman & Selingman 1932': 'Seligman & Seligman 1932',
                        'Santandrea 1944': 'Santandrea (1944-1945)',
                        'Santandrea 1945': 'Santandrea (1944-1945)',
                        'Métreaux 1928': 'Métraux 1928',
                        'Headland 1986': 'Headland pers. comm. (1986)',
                        'MArsden 1811': 'Marsden 1811',
                        'Frank 1959, Vol. 5': 'Frank (1959b)',
                        'Frank 1959, Vol. 1': 'Frank (1959a)',
                        'Bodwich 1873': 'Bowdich 1873',
                        'Rattray1927': 'Rattray 1927',
                        'Rattray 1937': 'Rattray 1927',
                        'Richard 1928': 'Reichard 1928',
                        'Richard 1950': 'Reichard 1950',
                        'Morgan 1901, Vol. 1': 'Morgan (1901)',
                        'Morgan 1901, Vol. 2': 'Morgan (1901)',
                    }.get(key, key)
                    key = {
                        ('Evans-Pritchard 1940', 'CARNEIRO6_037_Nuer.Sheet1'): 'Evans-Pritchard (1940b)',
                        ('Métreaux 1948', 'CARNEIRO6_048_Guarani.Sheet1'): 'Métraux 1948d',
                        ('Métraux 1948', 'CARNEIRO6_042_Tupinamba.Sheet1'): 'Métraux 1948c',
                        ('Smith 1925', 'CARNEIRO6_018_Ao_Naga.Sheet1'): 'Smith (1925b)',
                        ('Huntingford 1953', 'CARNEIRO6_022_Masai.Sheet1'): 'Huntingford (1953b)',
                        ('Fortes 1949', 'CARNEIRO6_006_Ashanti.Sheet1'): 'Fortes (1949b)',
                        ('Koppert 1930', 'CARNEIRO6_025_Nootka.Sheet1'): 'Koppert (1930b)',
                        ('Wallace 1947', 'CARNEIRO6_041_Delaware.Sheet1'): 'Wallace (1947a)',
                        ('Cooper 1946', 'CARNEIRO6_070_Ona.Sheet1'): 'Cooper (1946b)',
                        ('Swanton 1928', 'CARNEIRO6_017_Creek.Sheet1'): 'Swanton (1928a)',
                        ('Swanton 1928s', 'CARNEIRO6_017_Creek.Sheet1'): 'Swanton (1928a)',
                    }.get((key, p.stem), key)
                    assert key in key2bib, (key, p.stem)
                    src = key2bib[key]
                    if pages:
                        src += '[{}]'.format(pages.strip().replace(';', ','))
                    source.append(src)
                sid = '_'.join(p.stem.split('_')[:2])
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
