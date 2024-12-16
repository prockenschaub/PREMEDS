from MEDS_transforms.extract.utils import get_supported_fp
from omegaconf import OmegaConf, DictConfig
from pathlib import Path
import polars as pl

from functools import reduce

cfg = OmegaConf.load("configs/mimiciii-demo.yaml")
data_dir = Path("/Users/patrick/datasets/mimiciii-demo/1.4")
out_dir = Path("tmp")


ids = cfg['ids']
tbls = cfg['tbls']


def parse_tbl_cfg(name: str, cfg: DictConfig | None = None) -> DictConfig:
    if cfg is None:
        cfg = OmegaConf.create({'files': name})
    elif "files" not in cfg.keys():
        cfg["files"] = name
    return cfg

def col_cfg_to_polars_schema(cfg: DictConfig) -> pl.Schema:
    mapping = {name: getattr(pl, spec['type'])() for name, spec in cfg.items()}
    return pl.Schema(mapping)
    
tbls = {}
for name, tbl_cfg in cfg['tbls'].items():
    tbl_cfg = parse_tbl_cfg(name, tbl_cfg)
    tbl_fp, reader = get_supported_fp(data_dir, tbl_cfg['files']) # TODO: allow for multiple files

    # TODO: determine how to denote time variables

    schema = None
    if "cols" in tbl_cfg:
        # TODO: determine how to read in datetimes in any format
        schema = col_cfg_to_polars_schema(tbl_cfg['cols'])

    tbls[name] = reader(tbl_fp, schema=schema)

ids = None
prev_col = []
for id_type, id_cfg in cfg['ids'].items():
    # ASSUMPTION: IDs are always nested, first ID is the highest
    # TODO: combine into one mapping file
    id_col = id_cfg['col']
    start_col = id_cfg['start']
    end_col = id_cfg['end']
    tbl = id_cfg['tbl']
    
    id = tbls[tbl].select([id_col, start_col, end_col] + prev_col)
    id = id.rename({
        id_col: id_type,
        start_col: f'{id}'
    })

    if ids is None: 
        ids = id
    else:
        ids = ids.join(id, on=prev_col)
    
    prev_col = [id_col]

ids.collect()

ids['patient'].join(ids['hadm'], on="subject_id").collect()



reduce()